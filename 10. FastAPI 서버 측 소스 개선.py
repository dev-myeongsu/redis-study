import asyncio
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse


async def redis_listener(app: FastAPI):
    """[핵심] Redis 채널을 구독하고 메시지를 앱 내부 클라이언트들에게 전달하는 단일 루프"""

    pubsub = app.state.redis.pubsub()
    await pubsub.subscribe("system:notices")

    try:
        while True:
            # 메시지 대기
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )

            if message and message["type"] == "message":
                data = message["data"]

                # [개선] 순회 중 집합 변경으로 인한 에러 방지를 위해 list() 사본으로 순회
                if app.state.connected_clients:
                    for client_queue in list(app.state.connected_clients):
                        # 큐가 가득 찼을 경우를 대비해 비블로킹으로 전송 시도
                        try:
                            client_queue.put_nowait(data)
                        except asyncio.QueueFull:
                            pass

            await asyncio.sleep(0.01)

    except asyncio.CancelledError:
        # 태스크 종료 시 정상 흐름으로 간주하고 전파
        raise

    except Exception as e:
        print(f"Listener Error: {e}")

    finally:
        await pubsub.unsubscribe("system:notices")
        await pubsub.close()


@asynccontextmanager
async def lifespan(app: FastAPI):

    # 1. Redis 연결 및 클라이언트 관리 셋업
    app.state.redis = redis.from_url("redis://localhost:6379/0", decode_responses=True)

    # Redis에 비밀번호가 설정된 경우
    # app.state.redis = redis.from_url("redis://default:<비밀번호>@localhost:6379/0", decode_responses=True)

    app.state.connected_clients = set()  # 전역보다 구조적인 app.state 활용

    # 2. 백그라운드에서 단일 리스너 실행
    listener_task = asyncio.create_task(redis_listener(app))

    yield

    # 3. [개선] 종료 시 리스너 중단 및 명시적 종료 대기
    listener_task.cancel()
    try:
        await listener_task

    except asyncio.CancelledError:
        pass

    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/pub_sub")
async def index():
    return FileResponse("pub_sub.html")


@app.post("/publish-notice")
async def send_notice(message: str, request: Request):
    """관리자가 공지를 발행합니다."""

    await request.app.state.redis.publish("system:notices", message)
    return {
        "status": "sent",
        "active_clients": len(request.app.state.connected_clients),
    }


@app.get("/stream-notices")
async def stream_notices(request: Request):
    """클라이언트 연결 시 내부 큐를 생성하고 관리합니다."""

    # [설명] 실무에서는 큐 크기 제한(maxsize)을 두어 느린 클라이언트로 인한 메모리 폭증을 방지합니다.
    client_queue = asyncio.Queue(maxsize=100)
    request.app.state.connected_clients.add(client_queue)

    async def event_generator():
        try:
            while True:
                try:
                    # 5초 간격으로 연결 상태 확인을 위한 wait_for
                    data = await asyncio.wait_for(client_queue.get(), timeout=5.0)
                    yield f"data: {data}\n\n"

                except asyncio.TimeoutError:
                    pass

                if await request.is_disconnected():
                    break
        finally:
            # [개선] remove() 대신 discard()를 사용하여 이미 제거된 경우의 예외 방지
            request.app.state.connected_clients.discard(client_queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
