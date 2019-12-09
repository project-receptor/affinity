import asyncio
import time
import sys

import click
import requests
from prometheus_client.parser import text_string_to_metric_families
from receptor import Controller
from receptor import ReceptorConfig

from .topology import Topology


DEBUG = False

RECEPTOR_METRICS = (
    "active_work",
    "connected_peers",
    "incoming_messages",
    "route_events",
    "work_events",
)


def do_loop(topology):
    topology.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        topology.stop()


@click.group(
    help="Helper commands to check receptor's binding affinity."
)
def cli():
    pass


@cli.group(help="Commands to check receptor's behavior.")
def check():
    pass


@check.command("random")
@click.option("--socket-path", default=None)
@click.option("--debug", is_flag=True, default=False)
@click.option("--controller-port", help="Chooses Controller port", default=8888)
@click.option("--node-count", help="Choose number of nodes", default=10)
@click.option("--max-conn-count", help="Choose max number of connections per node", default=2)
@click.option("--profile", is_flag=True, default=False)
def randomize(controller_port, node_count, max_conn_count, debug, profile, socket_path):
    if debug:
        global DEBUG
        DEBUG = True

    topology = Topology.generate_random_mesh(
        controller_port, node_count, max_conn_count, profile, socket_path
    )
    print(topology)
    do_loop(topology)


@check.command("flat")
@click.option("--socket-path", default=None)
@click.option("--debug", is_flag=True, default=False)
@click.option("--controller-port", help="Chooses Controller port", default=8888)
@click.option("--node-count", help="Choose number of nodes", default=10)
@click.option("--profile", is_flag=True, default=False)
def flat(controller_port, node_count, debug, profile, socket_path):
    if debug:
        global DEBUG
        DEBUG = True

    topology = Topology.generate_flat_mesh(controller_port, node_count, profile, socket_path)
    print(topology)
    do_loop(topology)


@check.command("file")
@click.option("--debug", is_flag=True, default=False)
@click.argument("filename", type=click.File("r"))
def file(filename, debug):
    topology = Topology.load_topology_from_file(filename)
    do_loop(topology)


@check.command("ping")
@click.option("--validate", default=None)
@click.option("--count", default=10)
@click.option("--socket-path", default=None)
@click.argument("filename", type=click.File("r"))
def ping(filename, count, validate, socket_path):
    topology = Topology.load_topology_from_file(filename)

    results = topology.ping(count, socket_path=socket_path)
    Topology.validate_ping_results(results, validate)


@check.command("stats")
@click.option("--debug", is_flag=True, default=False)
@click.option("--profile", is_flag=True, default=False)
@click.argument("filename", type=click.File("r"))
def check_stats(filename, debug, profile):
    topology = Topology.load_topology_from_file(filename)
    failures = []

    for node in topology.nodes.values():
        if not node.stats_enable:
            continue
        stats = requests.get(f"http://localhost:{node.stats_port}/metrics")
        metrics = {
            metric.name: metric
            for metric in text_string_to_metric_families(stats.text)
            if metric.name in RECEPTOR_METRICS
        }
        expected_connected_peers = len(
            [n for n in topology.nodes.values() if node.name in n.connections]
        ) + len(node.connections)
        connected_peers = metrics["connected_peers"].samples[0].value
        if expected_connected_peers != connected_peers:
            failures.append(
                f"Node '{node.name}' was expected to have "
                f"{expected_connected_peers} connections, but it reported to "
                f" have {connected_peers}"
            )
    if failures:
        print("\n".join(failures))
        sys.exit(127)


@cli.group(help="Commands to run receptor with different behaviors.")
def receptor():
    pass


def run_as_ping(config):
    def ping_iter():
        if config.ping_count:
            for x in range(config.ping_count):
                yield x
        else:
            while True:
                yield 0

    async def ping_entrypoint():
        read_task = controller.loop.create_task(read_responses())
        await controller.add_peer(config.ping_peer)
        start_wait = time.time()
        while not controller.receptor.router.node_is_known(config.ping_recipient) and (
            time.time() - start_wait < 5
        ):
            await asyncio.sleep(0.1)
        await send_pings()
        await read_task

    async def read_responses():
        for _ in ping_iter():
            payload = await controller.recv()
            print("{}".format(payload))

    async def send_pings():
        for _ in ping_iter():
            await controller.ping(config.ping_recipient)
            await asyncio.sleep(config.ping_delay)

    print(f"Sending ping to {config.ping_recipient} via {config.ping_peer}.")
    controller = Controller(config)
    # controller.loop = asyncio.get_event_loop()
    return controller.run(ping_entrypoint)


@receptor.command("ping")
@click.option("--data-path", default="/tmp/receptor")
@click.option("--peer", default="receptor://127.0.0.1:8889")
@click.option("--id", default="node1")
@click.option("--node-id", default="ping-node")
@click.option("--count", default=10)
def receptor_ping(data_path, peer, id, node_id, count):
    config = ReceptorConfig(
        ["-d", data_path, "--node-id", node_id, "ping", "--peer", peer, id, "--count", str(count)]
    )
    run_as_ping(config)
