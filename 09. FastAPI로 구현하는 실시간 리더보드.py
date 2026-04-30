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

RANKING_KEY = "leaderboard:daily:2026-03-13"


@app.post("/rank/score")
async def update_score(user_id: str, score_delta: float, request: Request):
    """유저 점수를 실시간으로 업데이트합니다."""

    rd = request.app.state.redis

    # ZINCRBY: 기존 점수에 합산 (키나 멤버가 없으면 자동 생성)
    new_score = await rd.zincrby(RANKING_KEY, score_delta, user_id)

    return {"user_id": user_id, "current_score": new_score}


@app.get("/rank/top10")
async def get_top_rankers(request: Request):
    """상위 10명의 정보를 최신 문법(REV)으로 가져옵니다."""

    rd = request.app.state.redis

    # redis-py에서는 zrange 메서드의 desc=True 옵션이 REV 문법으로 동작합니다.
    top_list = await rd.zrange(RANKING_KEY, 0, 9, desc=True, withscores=True)

    # 튜플의 리스트를 가공된 JSON 배열로 변환
    result = [
        {"rank": i + 1, "user_id": m, "score": s} for i, (m, s) in enumerate(top_list)
    ]

    return {"top_rankers": result}


@app.get("/rank/around-me/{user_id}")
async def get_nearby_rank(user_id: str, request: Request):
    """특정 유저를 기준으로 앞뒤 유저를 포함한 '내 주변 5명'을 조회합니다."""

    rd = request.app.state.redis

    # 1. 나의 현재 등수 확인 (0-indexed)
    my_rank = await rd.zrevrank(RANKING_KEY, user_id)

    if my_rank is None:
        raise HTTPException(status_code=404, detail="Ranking data not found")

    # 2. 내 주변 범위 계산 (내 앞 2명 ~ 내 뒤 2명)
    start = max(0, my_rank - 2)
    end = my_rank + 2

    nearby_list = await rd.zrange(RANKING_KEY, start, end, desc=True, withscores=True)

    result = [
        {"rank": start + i + 1, "user_id": m, "score": s}
        for i, (m, s) in enumerate(nearby_list)
    ]

    return {"user_id": user_id, "nearby_rankers": result}
