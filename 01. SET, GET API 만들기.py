from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request  # 비동기 모듈을 임포트합니다.


@asynccontextmanager
async def lifespan(app: FastAPI):

    # 1. 서버 시작 시: Redis 연결 풀(Pool) 생성 후 app.state에 저장합니다.
    app.state.redis = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    print("🟢 Redis 연결 성공!")

    yield

    # 2. 서버 종료 시: Redis 연결 안전하게 해제합니다.
    await app.state.redis.aclose()
    print("🔴 Redis 연결 해제!")


# lifespan을 FastAPI 앱에 등록합니다.
app = FastAPI(lifespan=lifespan)


@app.post("/items/{item_id}")
async def set_items(item_id: str, value: str, request: Request):
    """Redis에 데이터를 저장합니다. (SET)"""

    # Request 객체를 통해 app.state에 저장된 레디스 연결을 가져옵니다.
    rd = request.app.state.redis
    key = f"item:{item_id}"

    # await rd.set(키, 값)
    await rd.set(key, value)

    return {"message": "Data saved to Redis successfully", "key": key, "value": value}


@app.get("/items/{item_id}")
async def get_item(item_id: str, request: Request):
    """Redis에서 데이터를 조회합니다. (GET)"""

    rd = request.app.state.redis
    key = f"item:{item_id}"

    # await rd.get(키)
    result = await rd.get(key)

    # Redis에 데이터가 없으면 None을 반환합니다.
    if result is None:
        return {"error": "Item not found in Redis"}

    return {"key": key, "value": result}
