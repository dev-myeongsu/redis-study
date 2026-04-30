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


@app.post("/articles/{article_id}/view")
async def increase_view_count(article_id: str, request: Request):

    rd = request.app.state.redis
    view_key = f"article:{article_id}:views"

    # [응용] 중복 조회수 방지 (Sets 조합)
    # 단순히 INCR만 쓰면 한 유저가 새로고침(F5)을 100번 할 때마다 숫자가 100 올라갑니다.
    # 이를 막으려면 Sets를 함께 조합해야 합니다.

    # 1. Sets에 유저 ID 추가 시도
    # 반환값이 1이면 새로운 유저(성공), 0이면 이미 존재하는 유저(실패)
    """
    is_new_viewer = await rd.sadd(f"article:{article_id}:viewers", user_id)
    """

    # 2. 새로운 유저일 때만 조회수 1 증가
    """
    if is_new_viewer == 1:
        await rd.incr(f"article:{article_id}:views")
    """

    # 1. 원자적으로 값 증가 (INCR)
    # 별도로 값을 GET 해서 더할 필요가 없습니다. (Race Condition 완벽 차단)
    current_views = await rd.incr(view_key)

    # 2. 특정 수치마다 DB에 백업하는 로직 (선택 사항)
    # 예: 조회수가 100 단위로 오를 때만 실제 메인 DB(RDB)에 쿼리를 실행하여 동기화
    if current_views % 100 == 0:
        print(
            f"📢 [Backup] Article {article_id} reached {current_views} views. Syncing to DB..."
        )

    return {"article_id": article_id, "total_views": current_views}


@app.get("/articles/{article_id}/stats")
async def get_article_stats(article_id: str, request: Request):

    rd = request.app.state.redis
    view_key = f"article:{article_id}:views"
    like_key = f"article:{article_id}:likes"

    # 여러 카운터 값을 가져오기
    # 데이터가 없을 경우(None)을 대비해 int 변환 및 기본값 0 처리
    views = await rd.get(view_key)
    likes = await rd.get(like_key)

    return {
        "article_id": article_id,
        "views": int(views) if views else 0,
        "likes": int(likes) if likes else 0,
    }
