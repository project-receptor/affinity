import uuid

import pytest

import receptor_affinity
from receptor_affinity.exceptions import RouteMismatchError, NodeUnavailableError
from receptor_affinity.mesh import Mesh, Node


def test_version():
    assert hasattr(receptor_affinity, "__version__")


def test_str_stopped_node_error():
    """Call ``str`` on a ``NodeUnavailableError``."""
    node = Node(str(uuid.uuid4()))
    err = NodeUnavailableError(node)
    str(err)


def test_str_route_mismatch_error():
    """Call ``str`` on a ``RouteMismatchError``."""
    # Node.__init__ doesn't require all required attributes. It should be fixed, but in the
    # meantime, using Node.create_from_config works around this issue, as it provides many default
    # values.
    node = Node.create_from_config({"name": str(uuid.uuid4())})
    node.start()
    mesh = Mesh()
    mesh.add_node(node)
    err = RouteMismatchError(mesh, (node,))
    str(err)


def test_node_pid_v1():
    """Call ``Node.pid`` on a not-yet-started node.

    Assert ``NodeUnavailableError`` is raised.
    """
    node = Node(str(uuid.uuid4()))
    with pytest.raises(NodeUnavailableError):
        node.pid


def test_node_pid_v2():
    """Call ``Node.pid`` on a started node.

    Assert an int is returned.
    """
    node = Node(str(uuid.uuid4()))
    node.start()
    try:
        node_pid = node.pid
        assert isinstance(node_pid, int)
    finally:
        node.stop()


def test_node_pid_v3():
    """Call ``Node.pid`` on a node that has been started and stopped.

    Assert ``NodeUnavailableError`` is raised.
    """
    node = Node(str(uuid.uuid4()))
    node.start()
    node.stop()
    with pytest.raises(NodeUnavailableError):
        node.pid


def test_node_pgid_v1():
    """Call ``Node.pgid`` on a not-yet-started node.

    Assert ``NodeUnavailableError`` is raised.
    """
    node = Node(str(uuid.uuid4()))
    with pytest.raises(NodeUnavailableError):
        node.pgid


def test_node_pgid_v2():
    """Call ``Node.pgid`` on a started node.

    Assert an int is returned.
    """
    node = Node(str(uuid.uuid4()))
    node.start()
    try:
        node_pgid = node.pgid
        assert isinstance(node_pgid, int)
    finally:
        node.stop()


def test_node_pgid_v3():
    """Call ``Node.pgid`` on a node that has been started and stopped.

    Assert ``NodeUnavailableError`` is raised.
    """
    node = Node(str(uuid.uuid4()))
    node.start()
    node.stop()
    with pytest.raises(NodeUnavailableError):
        node.pgid
