import hashlib
import random
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel


@asynccontextmanager
async def lifespan(app: FastAPI):

    # 1. 서버 시작 시: Redis 연결 풀(Pool) 생성 후 app.state에 저장합니다.
    app.state.redis = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    print("🟢 Redis 연결 성공!")

    # Redis에 비밀번호가 설정된 경우
    # app.state.redis = redis.from_url("redis://default:<비밀번호>@localhost:6379/0", decode_responses=True)

    yield

    # 2. 서버 종료 시: Redis 연결 안전하게 해제합니다.
    await app.state.redis.aclose()
    print("🔴 Redis 연결 해제!")


app = FastAPI(lifespan=lifespan)

# 인증번호 유효 시간 (300초 = 5분)
AUTH_TIMEOUT = 300


class SendCodeRequest(BaseModel):
    phone: str


class VerifyCodeRequest(BaseModel):
    phone: str
    input_code: str


@app.post("/auth/send")
async def send_verification_code(req_data: SendCodeRequest, request: Request):

    rd = request.app.state.redis

    # 1. 6자리 난수 생성
    code = str(random.randint(100000, 999999))

    # [보안] 전화번호 등 개인정보(PII)는 그대로 키로 쓰지 않고 해싱하여 저장합니다.
    hashed_phone = hashlib.sha256(req_data.phone.encode()).hexdigest()

    # 도배 방지 (Rate Limiting)
    # 인증번호 문자(SMS) 발송은 건당 비용이 발생합니다.
    # 누군가 악의적으로 1초에 100번씩 요청하면 금전적인 손실과 보안 위험이 큽니다.
    # 1분 이내에 같은 번호로 요청이 오면 거절하는 방어 로직 예시
    """
    is_allowed = await rd.set(f"auth:limit:{hashed_phone}", "1", ex=60, nx=True)

    if not is_allowd:
        raise HTTPException(status_code=429, detail="Please wait 1 minute before requesting again.")
    """

    cache_key = f"auth:code:{hashed_phone}"

    # 2. Redis에 저장 (Key: 해싱된 전화번호, Value: 인증번호, TTL: 300초)
    await rd.set(cache_key, code, ex=AUTH_TIMEOUT)

    # 3. 실제로는 여기에 SMS 발송 처리를 합니다.
    print(f"📧 [SMS 발송] To: {req_data.phone}, Code: {code}")

    return {
        "message": "Verification code sent",
        "code": code,
        "expires_in": AUTH_TIMEOUT,
    }


@app.post("/auth/verify")
async def verify_code(req_data: VerifyCodeRequest, request: Request):

    rd = request.app.state.redis

    # 동일하게 전화번호를 해싱하여 키를 생성합니다.
    hashed_phone = hashlib.sha256(req_data.phone.encode()).hexdigest()
    cache_key = f"auth:code:{hashed_phone}"

    # 1. Redis에서 해당 번호의 값 조회
    saved_code = await rd.get(cache_key)

    # Redis 6.2 이상일 경우 GETDEL 사용 권장
    # 단, 사용자 실수로 오타를 낸 경우에도 다시 인증번호를 발급 받아야 함
    # 아래 🚨[2026.03.20 Update] 코멘트 확인
    """
    🚨 [2026.03.20 Update] GETDEL 명령어

        GETDEL 명령어는 GET과 DEL을 동시에 실행해서 조회와 삭제 사이에 다른 요청이 개입할
        수 없도록 하여 원자성을 보장합니다. 다만 현재 샘플 구조에서 GETDEL을 사용하면 값을
        읽어옴과 동시에 키가 바로 삭제되므로, 유저가 실수로 오타를 냈을 때 재시도할 기회조차 없
        이 바로 폐기되어 새 인증번호를 요청해야만 합니다.

        GETDEL은 아주 엄격한 보안이 요구되어 "단 1회의 비교 시도만 허용(일치 여부와 무관하게
        즉시 파기)"하는 완전한 일회성 토큰의 원자성을 보장하고 싶을 때 사용하면 좋은 명령어입
        니다. 하지만 그렇지 않은 일반적인 서비스에서는 사용자 편의성(UX)을 저하시킬 수 있습니
        다.

        현재 샘플 구조에서 원자성과 사용자 편의성(UX)를 모두 만족시키려면 Lua Script를 이용
        해서 ‘인증번호 조회 후 일치할 경우 키 삭제’ 하는 로직을 구현하는 것이 정석입니다.
    """
    # saved_code = await rd.getdel(cache_key)

    # 2. 값이 없으면 만료되었거나 생성된 적이 없는 것
    if not saved_code:
        raise HTTPException(status_code=400, detail="Code expired or not requested")

    # 3. 입력값 비교
    if saved_code != req_data.input_code:
        raise HTTPException(status_code=400, detail="Invalid code")

    # 4. [핵심] 인증 성공 시 보안을 위해 즉시 삭제
    await rd.delete(cache_key)

    return {"message": "Authentication successful"}
