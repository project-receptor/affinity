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
    """
    A DiagNode takes a datapath, a node_id and a listener port and spins up an instance of a
    receptor node, exposing a few of the internal functions of receptor via an API so that a user
    testing a mesh can gain an insight into some of the internals.

    The three arguments are the same arguments one would pass to receptor itself.

    data_path: points to the data path where the manifest and message store will reside
    listen: defines the url that the receptor server will start on
    node_id: declares the name of the node, or the node_id

    The API exposes 4 endpoints:

    add_peer - takes a single peer argument that defines the url to a receptor node to connect to
    start - starts the receptor instances server portion
    ping - takes two arguments, recipient and count and will ping the recipient for count
    connections - returns a list of connections/routes the node knows about

    Initially, the server connects to nothing and calls to the API are required to connect the Diag
    Node to the mesh. The add_peer call will add the node to a particular peer and the Diag Node
    instance will become a part of the mesh.
    """
    def __init__(self, data_path, node_id, listen):
        # Set a random UUID to make our datapath unique each runtime
        self.uuid = uuid.uuid4()

        self.controller = None
        self.data_path = data_path or f"/tmp/receptor/{self.uuid}"
        self.listen = listen or f"receptor://127.0.0.1:{random_port()}"
        self.node_id = node_id

        # Use Receptor's own config object to create the config from the given options
        self.config = ReceptorConfig(["-d", data_path, "--node-id", node_id, "node"])

    async def _start(self, request):
        # Flasks start function, required to be async
        return self.start(request)

    def start(self, request):
        # Start the controller, enable the server and respond
        self.controller = Controller(self.config)
        print(self.listen)
        self.controller.enable_server([self.listen])
        return web.Response(text="Done")

    async def connections(self, request):
        # Returns the connections from the internal receptor router
        return web.Response(
            text=json.dumps(self.controller.receptor.router.get_edges())
        )

    async def add_peer(self, request):
        # Flask method that adds the peer to the controller
        peer = request.query.get("peer", "receptor://127.0.0.1:8889")
        self.controller.add_peer(peer)
        return web.Response(text="Done")

    async def run_as_ping(self, request):
        # Flask method that runs the ping command - stolen largely from Receptor core
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

    # Start the Receptor instance - but no connections are yet defined
    controller.start("")

    app = web.Application()
    app.add_routes([web.get("/start", controller._start)])
    app.add_routes([web.get("/add_peer", controller.add_peer)])
    app.add_routes([web.get("/ping", controller.run_as_ping)])
    app.add_routes([web.get("/connections", controller.connections)])

    # Start the flask app
    web.run_app(app, host=api_address, port=api_port)
