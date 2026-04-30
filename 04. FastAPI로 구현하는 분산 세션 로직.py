import uuid
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import Cookie, FastAPI, Request, Response
from pydantic import BaseModel


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

SESSION_EXPIRE = 3600  # 세션 만료 시간: 1시간(초)


class LoginRequest(BaseModel):
    user_id: str


@app.post("/login")
async def login(req_data: LoginRequest, response: Response, request: Request):

    rd = request.app.state.redis

    # 1. 고유하고 추측 불가능한 세션 ID 생성
    session_id = str(uuid.uuid4())
    session_key = f"session:{session_id}"

    # 2. 유저 정보를 Hashes로 저장
    user_info = {
        "user_id": req_data.user_id,
        "tier": "Premium",
        "ip": request.client.host if request.client else "127.0.0.1",
    }
    await rd.hset(session_key, mapping=user_info)

    # 3. 세션 만료 기간 설정 (1시간)
    await rd.expire(session_key, SESSION_EXPIRE)

    # 4. 클라이언트 쿠키에 세션 ID 전달 (보안을 위해 HttpOnly, Secure, SameSite 설정)
    # 주의: 로컬 개발 환경(HTTP)에서는 secure=False로 테스트해야 할 수 있습니다.
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        secure=False,  # 실제 운영환경에서는 True로 설정해야 합니다.
        samesite="lax",
    )

    return {"message": "Login Success", "session_id": session_id}


# Cookie(None)을 사용하려면 FastAPI가 클라이언트의 '쿠키'에서 해당 값을 자동으로 추출합니다.
@app.get("/me")
async def get_my_info(request: Request, session_id: str | None = Cookie(None)):

    if not session_id:
        return {"error": "Not logged in"}

    rd = request.app.state.redis
    session_key = f"session:{session_id}"

    # 1. 레디스에서 세션 정보 조회
    user_info = await rd.hgetall(session_key)

    if not user_info:
        return {"error": "Session expired or invalid"}

    # 2. 활동 중이므로 세션 시간 연장 (Sliding Window 방식)
    await rd.expire(session_key, SESSION_EXPIRE)

    return user_info
