import asyncio
import json
import logging
import time

from okx.websocket.WebSocketFactory import WebSocketFactory

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WsPublic")


class WsPublicAsync:
    def __init__(self, url):
        self.url = url
        self.subscriptions = set()
        self.callback = None
        self.loop = asyncio.get_event_loop()
        self.factory = WebSocketFactory(url)
        self.last_pong_time = None
        self.last_pong_count = 0

    async def connect(self):
        self.websocket = await self.factory.connect()

    # noinspection PyUnresolvedReferences
    async def consume(self):
        try:
            async for message in self.websocket:
                if message == 'pong':
                    self.last_pong_count += 1
                    self.last_pong_time = time.time()
                else:
                    if self.callback:
                        await self.callback(message)
        except Exception as e:
            logger.warning(f'Connection was closed: [{str(e)}].')

    async def subscribe(self, params: list, callback):
        self.callback = callback
        payload = json.dumps({
            "op": "subscribe",
            "args": params
        })
        await self.websocket.send(payload)
        # await self.consume()

    async def unsubscribe(self, params: list, callback):
        self.callback = callback
        payload = json.dumps({
            "op": "unsubscribe",
            "args": params
        })
        logger.info(f"unsubscribe: {payload}")
        await self.websocket.send(payload)

    async def stop(self):
        await self.factory.close()
        self.loop.stop()

    async def start(self):
        logger.info("Connecting to WebSocket...")
        await self.connect()
        self.loop.create_task(self.consume())

    def stop_sync(self):
        self.loop.run_until_complete(self.stop())
