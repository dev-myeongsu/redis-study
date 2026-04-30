import asyncio
import time
import uuid
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request


@asynccontextmanager
async def lifespan(app: FastAPI):

    # 1. 서버 시작 시: Redis 연결 풀(Pool) 생성 후 app.state에 저장합니다.
    app.state.redis = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    print("🟢 Redis 연결 성공!")

    yield

    # 2. 서버 종료 시: Redis 연결 안전하게 해제합니다.
    await app.state.redis.aclose()
    print("🔴 Redis 연결 해제!")


app = FastAPI(lifespan=lifespan)


# --- Lock 획득 로직 ---
async def acquire_lock(
    rd, lock_name: str, acquire_timeout: float = 10.0, lock_timeout_ms: int = 5000
):
    """
    acquire_timeout: Lock을 얻기 위해 무한정 기다리지 않고 포기할 최대 대기 시간 (초)
    lock_timeout_ms: Lock 자체의 유효 시간 (밀리초)
    """

    identifier = str(uuid.uuid4())  # 내가 잡은 락임을 증명하는 고유 토큰
    end_time = time.time() + acquire_timeout

    while time.time() < end_time:
        # NX: 키가 없을 때만 생성, PX: 밀리초 단위 TTL 생성
        if await rd.set(lock_name, identifier, nx=True, px=lock_timeout_ms):
            return identifier

        # Lock 획득 실패 시 다른 서버가 풀 때까지 0.1초 대기 후 재시도 (Spin Lock)
        await asyncio.sleep(0.1)

    return False


# --- Lock 해제 로직 ---
async def release_lock(rd, lock_name: str, identifier: str):

    # [주의] 실제 환경에서는 GET과 DEL 사이의 원자성을 위해 Lua 스크립트를 사용해야 완벽합니다.
    # 본 실습에서는 이해를 돕기 위해 파이썬 레벨에서 검증 후 삭제합니다.
    if await rd.get(lock_name) == identifier:
        await rd.delete(lock_name)
        return True
    return False


@app.post("/stock/reduce/{item_id}")
async def reduce_stock(item_id: str, request: Request, user_id: str = "unknown"):

    rd = request.app.state.redis
    lock_name = f"lock:item:{item_id}"

    # 1. 락 획득 시도
    lock_id = await acquire_lock(rd, lock_name)
    if not lock_id:
        raise HTTPException(
            status_code=409,
            detail=f"현재 접속자가 많아 처리가 지연되고 있습니다. 다시 시도해주세요. (요청자: {user_id})",
        )

    try:
        # 2. 임계 영역 (Critical Section): 실제 DB 재고 차감 로직 수행
        # 서버 콘솔에서 어떤 유저의 요청이 처리 중인지 식별 가능하도록 로그 출력
        print(f"🟩 [Success] Item {item_id} 재고 차감 작업 중... (요청자: {user_id})")

        await asyncio.sleep(2)  # 무거운 비즈니스 로직 시뮬레이션

        return {"message": f"Item {item_id} stock reduced successfully for {user_id}"}

    finally:
        # 3. 예외가 발생하더라도 작업 완료 후 반드시 락을 해제해야 함!
        await release_lock(rd, lock_name, lock_id)
