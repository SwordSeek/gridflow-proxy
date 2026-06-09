"""
GridFlow — WebSocket Proxy для Binance Futures
Запускается на Fly.io (Amsterdam), пробрасывает fstream.binance.com
"""
import asyncio
import websockets
from websockets.server import serve
import logging
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

BINANCE_BASE = "wss://fstream.binance.com"


async def proxy_handler(client_ws):
    path = client_ws.request.path  # /stream?streams=dogeusdt@aggTrade
    binance_url = BINANCE_BASE + path
    log.info(f"Connect → {binance_url}")

    try:
        async with websockets.connect(binance_url) as binance_ws:

            async def fwd_to_client():
                async for msg in binance_ws:
                    await client_ws.send(msg)

            async def fwd_to_binance():
                async for msg in client_ws:
                    await binance_ws.send(msg)

            await asyncio.gather(fwd_to_client(), fwd_to_binance())

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"Error: {e}")


async def health(request):
    return web.Response(text="OK")


async def main():
    # HTTP health check на порту 8081
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8081)
    await site.start()

    # WebSocket прокси на порту 8080
    log.info("GridFlow WS Proxy started on :8080, health on :8081/health")
    async with serve(proxy_handler, "0.0.0.0", 8080):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
