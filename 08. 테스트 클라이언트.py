import asyncio

import httpx


async def send_request(client, req_num):

    # FastAPI 서버로 GET 요청 전송
    response = await client.get("http://localhost:8000/data")

    if response.status_code == 200:
        print(f"🟢 요청 {req_num:02d}: 성공! 통과됨")
    elif response.status_code == 429:
        print(
            f"🔴 요청 {req_num:02d}: 차단됨! (429 Too Many Requests) - {response.json()['retry_after']} 대기 필요"
        )
    else:
        print(f"🔴 요청 {req_num:02d}: 서버 에러 ({response.status_code})")


async def main():
    async with httpx.AsyncClient() as client:

        print("🚀 10개의 API 요청을 동시에 발사합니다...\n")

        # 10번을 0.01초 간격 없이 동시에 전송
        tasks = [send_request(client, i) for i in range(1, 11)]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
