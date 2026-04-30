import time
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


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


# 제한 설정: 1분당 최대 5회 (시연을 위해 짧게 설정)
LIMIT = 5
WINDOW = 60


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):

    # [Tip] Swagger UI(/docs) 접속은 제한에서 제외해야 테스트가 편합니다.
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    rd = request.app.state.redis

    # Rate limiting에서는 Key 설계가 중요합니다. (Rate Limiting 기준 식별자)
    # 서비스 정책에 따라 IP, user_id, API key 등을 사용할 수 있습니다.
    # ex) rate_limit:{ip}, rate_limit:{user_id}, rate_limit:{api_key}
    user_identifier = request.client.host if request.client else "127.0.0.1"

    # [핵심 로직] 현재 시간을 WINDOW(60초) 단위의 정수로 변환
    # 이 연산을 통해 0~59초 사이의 요청은 모두 동일한 식별자 (current_minute)를 가집니다.
    current_minute = int(time.time() // WINDOW)
    cache_key = f"rate_limit:{user_identifier}:{current_minute}"

    # 1. 카운트 증가 (SET 없이 바로 INCR!)
    count = await rd.incr(cache_key)

    # 2. 처음 생성된 키(값이 1)라면 만료 시간 설정
    if count == 1:
        await rd.expire(cache_key, WINDOW)

    # 3. 제한 초과 여부 확인
    if count > LIMIT:
        # [주의] FastAPI 미들웨어 안에서는 HTTPException을 raise하면 500 에러가 발생합니다.
        # 따라서 반드시 JSONResponse를 직접 반환해서 429 상태 코드를 내려주어야 합니다.
        return JSONResponse(
            status_code=429,
            content={
                "" "error": "Too Many Requests",
                "detail": f"1분에 {LIMIT}회까지만 요청 가능합니다.",
                "retry_after": f"{WINDOW - (int(time.time() % WINDOW))}s",
            },
        )

    # 4. 검사 통과 시 실제 API 라우터로 요청 전달
    response = await call_next(request)
    return response


@app.get("/data")
async def get_sensitive_data():
    return {"data": "This is protected by rate limiting"}


"""
Token Bucket을 구현한 파이썬 로직

    import time

    # [환경 설정]
    CAPACITY = 10  # 버킷 최대 용량 (한번에 허용 가능한 최대 횟수)
    REFILL_RATE = 1.0  # 초당 채워지는 토큰 수 (1초에 1개씩)


    def allow_request(user_id: str) -> bool:
    
        current_time = time.time()  # 현재 시간 (예: 1715432003.0)

        # 1. Redis에서 기존 상태 가져오기 (실제로는 HGETALL 등 사용)
        # 예: {"tokens": 5, "last_refill_time": 1715432000.0} (3초 전 상태)
        last_tokens = 5
        last_refill_time = 1715432000.0

        # 2. 시간 차이 계산
        time_passed = max(0, current_time - last_refill_time)  # 3초

        # 3. 그동안 생성되었어야 할 토큰 수 계산
        new_tokens = time_passed * REFILL_RATE  # 3.0 * 1.0 = 3개

        # 4. 토큰 더하기 (단, 최대 용량 CAPACITY를 넘을 수 없음)
        # min(10, 5 + 3) = 8개
        current_tokens = min(CAPACITY, last_tokens + new_tokens)

        # 5. 요청 처리 (토큰이 1개 이상이면 통과)
        if current_tokens >= 1.0:
            current_tokens -= 1.0
            # -> 이후 Redis에 갱신된 값(tokens: 7.0, last_refill_time: current_time)을
            # 다시 저장 (HSET)
            return True
        else:
            # 토큰이 부족하면 차단
            return False
"""
