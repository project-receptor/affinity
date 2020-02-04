import click
from receptor import ReceptorConfig

from .debugger import run_as_debugger
from .ping import run_as_ping


@click.group(help="Helper commands to check receptor's binding affinity.")
def cli():
    pass


@cli.group(help="Commands to run receptor with different behaviors.")
def receptor():
    pass


@receptor.command("debugnode")
@click.option("--data-path", default=None)
@click.option("--node-id", default="diag_node")
@click.option("--listen", default=None)
@click.option("--api-address", default=None)
@click.option("--api-port", default=None)
def debugnode(data_path, node_id, listen, api_address, api_port):
    run_as_debugger(data_path, node_id, listen, api_address, api_port)


@receptor.command("ping")
@click.option("--data-path", default="/tmp/receptor")
@click.option("--peer", default="receptor://127.0.0.1:8889")
@click.option("--id", default="node1")
@click.option("--node-id", default="ping-node")
@click.option("--count", default=10)
def ping(data_path, peer, id, node_id, count):
    config = ReceptorConfig(
        [
            "-d",
            data_path,
            "--node-id",
            node_id,
            "ping",
            "--peer",
            peer,
            id,
            "--count",
            str(count),
        ]
    )
    run_as_ping(config)
