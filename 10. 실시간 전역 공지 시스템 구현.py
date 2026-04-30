import asyncio
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse


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

# 공지사항용 채널명
NOTICE_CHANNEL = "system:notices"


@app.get("/pub_sub")
async def index():
    # pub_sub.html 파일을 웹 브러우저에 띄웁니다.
    return FileResponse("pub_sub.html")


@app.post("/publish-notice")
async def send_notice(message: str, request: Request):
    """[발행자] Swagger에서 공지를 발행합니다."""

    rd = request.app.state.redis

    # 채널에 메시지 전송 (구독 중인 수신자 수를 반환)
    subscriber_count = await rd.publish(NOTICE_CHANNEL, message)

    return {"status": "success", "received_subscribers": subscriber_count}


@app.get("/stream-notices")
async def stream_notices(request: Request):
    """[수신자] SSE를 통해 실시간 알림을 수신합니다."""

    async def event_generator():
        # [핵심] rd.pubsub()으로 구독 전용 객체 생성 (커넥션 분리)
        async with request.app.state.redis.pubsub() as pubsub:

            await pubsub.subscribe(NOTICE_CHANNEL)

            try:
                while True:
                    # 메시지 대기 (ignore_subscribe_messages=True로 설정)
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )

                    if message and message["type"] == "message":
                        data = message["data"]

                        # SSE 표준 포맷 전송 (data: [내용]\n\n)
                        yield f"data: {data}\n\n"

                    # 브라우저가 창을 닫으면 루프를 종료하여 커넥션 변환
                    if await request.is_disconnected():
                        break

                    await asyncio.sleep(0.01)

            finally:
                # 작업 종료 시 명시적 구독 해제
                await pubsub.unsubscribe(NOTICE_CHANNEL)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
