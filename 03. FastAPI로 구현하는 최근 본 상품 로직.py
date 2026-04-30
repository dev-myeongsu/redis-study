from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request


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


# 최근 본 상품 업데이트
@app.post("/products/{product_id}/view")
async def view_product(product_id: str, request: Request, user_id: str = "user_1"):

    rd = request.app.state.redis
    key = f"user:{user_id}:recent_views"

    # 1. 기존 리스트에서 동일한 ID가 있다면 제거 (중복 방지 및 끌어올리기)
    # LREM [key] [count] [value] : count가 0이면 일치하는 모든 항목을 삭제합니다.
    await rd.lrem(key, 0, product_id)

    # 2. 최신 상품 ID를 왼쪽(맨 앞)에 추가
    await rd.lpush(key, product_id)

    # 3. 최신 5개만 남기고 자르기 (Fixed_size 큐 유지)
    # 인덱스는 0부터 시작하므로 0~4는 총 5개를 의미합니다.
    await rd.ltrim(key, 0, 4)

    return {"message": f"Product {product_id} added to recent views"}


# 최근 본 상품 조회
@app.get("/users/{user_id}/recent-views")
async def get_recent_views(user_id: str, request: Request):

    rd = request.app.state.redis
    key = f"user:{user_id}:recent_views"

    # 리스트 전체(인덱스 0부터 -1까지) 조회
    views = await rd.lrange(key, 0, -1)

    return {"recent_views": views}
