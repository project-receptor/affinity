import atexit
import json
import os
import random
import signal
import subprocess
import time
import uuid
from collections import defaultdict
from urllib.parse import quote
from urllib.parse import urlparse

import attr
import psutil
import requests
import yaml
from prometheus_client.parser import text_string_to_metric_families
from typing import Dict, Iterator

from .exceptions import RouteMismatchError, NodeUnavailableError
from .utils import Conn
from .utils import random_port
from .utils import read_and_parse_metrics
from wait_for import wait_for, TimedOutError

STANDARD = 0
DIAG = 1

procs: Dict[uuid.UUID, subprocess.Popen] = {}


def shut_all_procs():
    for proc in procs.values():
        proc.kill()


atexit.register(shut_all_procs)


@attr.s
class Node:
    name = attr.ib()
    controller = attr.ib(default=False)
    # NOTE A receptor node can listen on multiple ports. This attr doesn't handle that possibility.
    listen = attr.ib(default=None)
    connections = attr.ib(factory=list)
    stats_enable = attr.ib(default=False)
    stats_port = attr.ib(default=None)
    profile = attr.ib(default=False)
    data_path = attr.ib(default=None)
    mesh = attr.ib(init=False, default=None)
    uuid = attr.ib(init=False, factory=uuid.uuid4)
    active = attr.ib(init=False)

    node_type = STANDARD

    def __attrs_post_init__(self):
        if not self.data_path:
            self.data_path = f"/tmp/receptor/{str(self.uuid)}"
        if not self.listen:
            self.listen = f"receptor://0.0.0.0:{random_port()}"

    @staticmethod
    def create_from_config(config):
        return Node(
            name=config["name"],
            controller=config.get("controller", False),
            listen=config.get("listen", f"receptor://0.0.0.0:{random_port()}"),
            connections=config.get("connections", []) or [],
            stats_enable=config.get("stats_enable", False),
            stats_port=config.get("stats_port", None) or random_port(),
            profile=config.get("profile", False),
            data_path=config.get("data_path", None),
        )

    def _construct_run_command(self):
        if self.profile:
            st = [
                "python",
                "-m",
                "cProfile",
                "-o",
                f"{self.name}.prof",
                "-m",
                "receptor.__main__",
            ]
        else:
            st = ["receptor"]

        if self.controller:
            st.extend(["-d", self.data_path, "--node-id", self.name, "controller"])
            st.extend([f"--listen={self.listen}"])
        else:
            st.extend(["-d", self.data_path, "--node-id", self.name, "node"])
            st.extend([f"--listen={self.listen}"])
            for pnode in self.connections:
                st.append(f"--peer={self.mesh.nodes[pnode].listen}")

        if self.stats_enable:
            st.extend(["--stats-enable", f"--stats-port={self.stats_port}"])

        return st

    def start(self, wait_for_ports=True):
        print(f"{time.time()} starting {self.name}({self.uuid})")
        op = subprocess.Popen(self._construct_run_command(), start_new_session=True)
        procs[self.uuid] = op

        if wait_for_ports:
            self.wait_for_ports()
        self.active = True

    def wait_for_ports(self) -> None:
        """Wait for this node to bind to ports.

        More specifically, wait for the receptor process corresponding to this object to bind to the
        port embedded in ``self.listen``. Raise ``TimedOutError`` if ports are not bound to within
        10 seconds.
        """
        wanted_ports = {self.listen_port}
        if self.stats_enable:
            wanted_ports.add(self.stats_port)
        wait_for(lambda: wanted_ports.issubset(set(self.bound_ports())), num_sec=10)

    def stop(self) -> None:
        """Kill this node with SIGKILL.

        If this node already appears to be dead, then do nothing.
        """
        try:
            pgid = self.pgid
        except NodeUnavailableError:
            return  # Can't find node PID

        # TODO NICE FOR DEBUGGER
        # TODO Shouldn't SIGTERM be tried before SIGKILL?
        os.killpg(pgid, signal.SIGKILL)
        procs[self.uuid].wait()
        self.active = False
        del procs[self.uuid]

    @property
    def pid(self) -> int:
        """Get this node's PID (process ID).

        Raises ``NodeUnavailableError`` if a PID can't be found.
        """
        try:
            return procs[self.uuid].pid
        except KeyError as err:
            raise NodeUnavailableError("Can't get PID for a stopped node.") from err

    @property
    def pgid(self) -> int:
        """Get this node's PGID (process group ID).

        Raises ``ProcessLookupError`` if a PGID can't be found.
        """
        return os.getpgid(self.pid)

    def bound_ports(self) -> Iterator[int]:
        """Yield all ports that this node has bound to.

        Raise ``NodeUnavailableError`` if connections can't be fetched. (Perhaps the backing
        receptor process has died?)
        """
        try:
            conns = psutil.Process(self.pid).connections()
        except psutil.AccessDenied as err:
            raise NodeUnavailableError(
                "Can't get conns for PID {self.pid}. Perhaps the PID has died?"
            ) from err
        for conn in conns:
            if conn.status == "LISTEN":
                yield conn.laddr.port

    @property
    def hostname(self):
        return urlparse(self.listen).hostname

    @property
    def listen_port(self) -> int:
        return urlparse(self.listen).port

    def get_metrics(self):
        stats = requests.get(f"http://{self.hostname}:{self.stats_port}/metrics")
        metrics = {
            metric.name: metric for metric in text_string_to_metric_families(stats.text)
        }
        return metrics

    def get_routes(self):
        routes = self.get_metrics()["routing_table_info"].samples[0].labels["edges"]
        if routes == "()":
            return set()
        else:
            return read_and_parse_metrics(routes)

    def validate_routes(self) -> None:
        if not self.active:
            raise NodeUnavailableError("Routes were requested from a stopped node.")
        if self.mesh.generate_routes() != self.get_routes():
            raise RouteMismatchError(self.mesh, (self,))

    def ping(self, count, peer=None, node_ping_name="ping_node"):

        if self.mesh.diag_node:
            return self.mesh.diag_node.ping(count=count, recipient=self.name)

        if not peer:
            peer = self.mesh.find_controller()[0]

        if node_ping_name not in self.mesh.nodes:
            self.mesh.add_node(DiagNode(name=node_ping_name))

        if peer.name not in self.mesh.nodes[node_ping_name].connections:
            self.mesh.nodes[node_ping_name].connections.append(peer.name)

        peer_address = self.mesh.nodes[peer.name].listen

        starter = [
            "time",
            "affinity",
            "receptor",
            "debugnode",
            "--data-path",
            self.data_path,
            "--node-id",
            node_ping_name,
            "--peer",
            peer_address,
            "--id",
            self.name,
            "--count",
            str(count),
        ]
        print(starter)
        start = time.time()
        op = subprocess.run(starter, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        duration = time.time() - start
        cmd_output = op.stdout.readlines()
        print(op.stderr.read())
        print(cmd_output)
        if b"Failed" in cmd_output[0]:
            return "Failed"
        else:
            return duration / count


@attr.s
class DiagNode(Node):
    api_address = attr.ib(default="0.0.0.0")
    api_port = attr.ib(default=8080)
    node_type = DIAG

    def _construct_run_command(self):
        starter = [
            "affinity",
            "receptor",
            "debugnode",
            "--listen",
            self.listen,
            "--data-path",
            self.data_path,
            "--api-address",
            self.api_address,
            "--api-port",
            str(self.api_port),
        ]
        print(starter)
        return starter

    def wait_for_ports(self):
        super().wait_for_ports()
        wanted_ports = {self.api_port}
        wait_for(lambda: wanted_ports.issubset(set(self.bound_ports())), num_sec=10)

    def validate_routes(self) -> None:
        if self.mesh.generate_routes() != self.get_routes():
            raise RouteMismatchError(self.mesh, (self,))

    def get_routes(self):
        route_data = json.loads(
            requests.get(f"http://{self.api_address}:{self.api_port}/connections").text
        )
        routes = set()
        for route in route_data:
            routes.add(Conn(route[0], route[1], route[2]))
        return routes

    def ping(self, count=5, recipient="controller"):
        output = requests.get(
            f"http://{self.api_address}:{self.api_port}/ping?count={count}&recipient={recipient}"
        ).text

        if "Failed" in output:
            return "Failed"
        else:
            times = json.loads(output)
            return sum(times) / len(times)

    def add_peer(self, peer):
        coded_peer = quote(peer.listen)
        requests.get(
            f"http://{self.api_address}:{self.api_port}/add_peer?peer={coded_peer}"
        )

    def create_from_config(config):
        raise NotImplementedError


@attr.s
class Mesh:
    use_diag_node = attr.ib(default=False)
    nodes: Dict[str, Node] = attr.ib(init=False, factory=dict)
    diag_node = attr.ib(init=False, default=None)

    def __attrs_post_init__(self):
        if self.use_diag_node:
            self.diag_node = DiagNode(
                name="diag_node", listen=f"receptor://127.0.0.1:{random_port()}"
            )
            self.add_node(self.diag_node)

    def add_node(self, node):
        if node.name not in self.nodes:
            self.nodes[node.name] = node
            node.mesh = self
        else:
            raise Exception("Mesh already has a node by the same name")

    def remove_node(self, node_or_name):
        if isinstance(node_or_name, Node):
            node_name = node_or_name.name
        else:
            node_name = node_or_name
        if node_name not in self.nodes:
            raise Exception("Mesh has no node by that name")
        else:
            self.nodes[node_name].mesh = None
            del self.nodes[node_name]

    @staticmethod
    def gen(controller_port, node_count, conn_method, profile=False):
        mesh = Mesh()
        mesh.add_node(
            Node(
                name="controller",
                controller=True,
                listen=f"receptor://127.0.0.1:{controller_port}",
                profile=profile,
            )
        )

        for i in range(node_count):
            mesh.add_node(
                Node(
                    name=f"node{i}",
                    controller=False,
                    listen=f"receptor://127.0.0.1:{random_port()}",
                    profile=profile,
                )
            )

        for k, node in mesh.nodes.items():
            if node.controller:
                continue
            else:
                node.connections.extend(conn_method(mesh, node))
        return mesh

    @staticmethod
    def gen_random(controller_port, node_count, max_conn_count, profile):
        def peer_function(mesh, cur_node):
            nconns = defaultdict(int)
            print(mesh)
            for k, node in mesh.nodes.items():
                for conn in node.connections:
                    nconns[conn] += 1
            available_nodes = list(
                filter(lambda o: nconns[o] < max_conn_count, mesh.nodes)
            )
            print("------")
            print(nconns)
            print(available_nodes)
            print(cur_node.name)
            print(
                random.choices(available_nodes, k=int(random.random() * max_conn_count))
            )
            print("----")
            if cur_node.name not in available_nodes:
                return []
            else:
                return random.choices(
                    available_nodes, k=int(random.random() * max_conn_count)
                )

        mesh = Mesh.gen_random(controller_port, node_count, peer_function, profile)
        return mesh

    @staticmethod
    def gen_flat(controller_port, node_count, profile):
        def peer_function(*args):
            return ["controller"]

        mesh = Mesh.gen_random(controller_port, node_count, peer_function, profile)
        return mesh

    def dump_yaml(self, filename=".last-mesh.yaml"):
        with open(filename, "w") as f:
            data = {"nodes": {}}
            for node, node_data in self.nodes.items():
                data["nodes"][node] = {
                    "name": node_data.name,
                    "listen": node_data.listen if node_data.controller else None,
                    "controller": node_data.controller,
                    "connections": node_data.connections,
                    "stats_enable": node_data.stats_enable,
                    "stats_port": node_data.stats_port,
                }
                if node_data.data_path:
                    data["nodes"][node]["data_path"] = node_data.data_path

            yaml.dump(data, f)

    def generate_routes(self):
        routes = set()
        for node, node_data in self.nodes.items():
            for conn in node_data.connections:
                routes.add(Conn(node_data.name, conn, 1))
        return routes

    def generate_dot(self):
        dot_data = "graph {"
        for node, node_data in self.nodes.items():
            for conn in node_data.connections:
                dot_data += f"{node} -- {conn}; "
        dot_data += "}"
        return dot_data

    def start(self, wait=True):
        self.dump_yaml()

        for k, node in self.nodes.items():
            node.start(wait_for_ports=not wait)

        if wait:
            print("Waiting for nodes")
            for _, node in self.nodes.items():
                node.wait_for_ports()
            if self.use_diag_node:
                self.diag_node.add_peer(self.find_controller()[0])
                self.nodes[self.diag_node.name].connections.append(
                    self.find_controller()[0].name
                )
            self.settle()

    def stop(self):
        for k, node in self.nodes.items():
            node.stop()
        print("all killed")

    @staticmethod
    def load_from_file(filename, use_diag_node=False):
        with open(filename) as f:
            data = yaml.safe_load(f)

        mesh = Mesh(use_diag_node=use_diag_node)
        for node_name, definition in data["nodes"].items():
            node = Node.create_from_config(definition)
            mesh.add_node(node)

        return mesh

    def find_controller(self):
        return list(filter(lambda o: o.controller, self.nodes.values()))

    def ping(self, count=10):
        results = {}

        # Need to grab the list of nodes prior to running as pinging adds a node
        nodes = list(self.nodes.keys())
        for node_name in nodes:
            node = self.nodes[node_name]
            results[node.name] = node.ping(count)
        return results

    def settle(self) -> None:
        """Wait for the mesh and its nodes to settle on the same routes.

        If the nodes and mesh don't agree within approximately 30 seconds, throw
        ``RouteMismatchError``.
        """
        # There's two reasons to catch TimedOutError. First, it doesn't give terribly useful
        # diagnostic information, whereas RouteMismatchError (raised by validate_routes()) does.
        # Second, we want this library to hide implementation details, like the fact that we depend
        # on the wait-for package.
        #
        # It'd be nice if we could raise the original RouteMismatchError. Unfortunately,
        # TimedOutError doesn't hold a reference to the underlying exception.
        #
        # We don't call validate_routes() for a final time from within the except: block, so as to
        # make tracebacks more concise.
        settled = True
        try:
            wait_for(
                self.validate_routes,
                delay=6,
                num_sec=30,
                handle_exception=True,
                fail_condition=lambda result: isinstance(result, RouteMismatchError),
            )
        except TimedOutError:
            settled = False
        if not settled:
            self.validate_routes()

    @staticmethod
    def validate_ping_results(results, threshold=0.1):
        valid = True
        for node in results:
            print(f"Asserting node {node} was under {threshold} threshold")
            print(f"  {results[node]}")
            if results[node] == "Failed" or float(results[node]) > float(threshold):
                valid = False
        return valid

    def validate_routes(self) -> None:
        problem_nodes = []
        for node in self.nodes.values():
            if not node.active:
                continue
            try:
                node.validate_routes()
            except RouteMismatchError:
                problem_nodes.append(node)
        if problem_nodes:
            raise RouteMismatchError(self, problem_nodes)
