import asyncio
import json
import time
import uuid
import dateutil.parser

from aiohttp import web
from receptor import Controller
from receptor import ReceptorConfig
from receptor.exceptions import UnrouteableError

from .utils import random_port


class DiagNode:
    def __init__(self, data_path, node_id, listen):
        self.uuid = uuid.uuid4()
        self.controller = None
        self.data_path = data_path or f"/tmp/receptor/{self.uuid}"
        self.listen = listen or f"receptor://127.0.0.1:{random_port()}"
        self.node_id = node_id
        self.config = ReceptorConfig(["-d", data_path, "--node-id", node_id, "node"])

    async def _start(self, request):
        self.start(request)

    def start(self, request):
        self.controller = Controller(self.config)
        print(self.listen)
        self.controller.enable_server([self.listen])
        return web.Response(text="Done")

    async def connections(self, request):
        return web.Response(
            text=json.dumps(self.controller.receptor.router.get_edges())
        )

    async def add_peer(self, request):
        peer = request.query.get("peer", "receptor://127.0.0.1:8889")
        self.controller.add_peer(peer)
        return web.Response(text="Done")

    async def run_as_ping(self, request):
        recipient = request.query.get("recipient")
        count = int(request.query.get("count", 5))
        responses = []

        def ping_iter():
            if count:
                for x in range(count):
                    yield x
            else:
                while True:
                    yield 0

        async def ping_entrypoint():
            read_task = self.controller.loop.create_task(read_responses())
            start_wait = time.time()
            while not self.controller.receptor.router.node_is_known(recipient) and (
                time.time() - start_wait < 5
            ):
                await asyncio.sleep(0.01)
            await send_pings()
            await read_task

        async def read_responses():
            for _ in ping_iter():
                message = await self.controller.recv()
                payload = message.raw_payload
                dta = json.loads(payload)
                duration = dateutil.parser.parse(
                    dta["response_time"]
                ) - dateutil.parser.parse(dta["initial_time"])
                responses.append(duration.total_seconds())
                print(duration.total_seconds())
                print("{}".format(payload))

        async def send_pings():
            for _ in ping_iter():
                await self.controller.ping(recipient)
                await asyncio.sleep(0.1)

        print(f"Sending ping to {recipient} via {'something'}.")

        # controller.loop = asyncio.get_event_loop()
        # return controller.run(ping_entrypoint)
        try:
            await asyncio.wait_for(ping_entrypoint(), timeout=10)
        except UnrouteableError:
            return web.Response(text="Failed", status=404)
        except ConnectionRefusedError:
            return web.Response(text="Failed", status=404)
        except asyncio.TimeoutError:
            return web.Response(text="Failed", status=404)
        print("RESPONSE")
        return web.Response(text=json.dumps(responses))


def run_as_debugger(data_path, node_id, listen, api_address, api_port):
    controller = DiagNode(data_path, node_id, listen)
    controller.start("")
    app = web.Application()
    app.add_routes([web.get("/start", controller._start)])
    app.add_routes([web.get("/add_peer", controller.add_peer)])
    app.add_routes([web.get("/ping", controller.run_as_ping)])
    app.add_routes([web.get("/connections", controller.connections)])

    web.run_app(app, host=api_address, port=api_port)
