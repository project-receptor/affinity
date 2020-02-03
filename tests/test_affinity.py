import receptor_affinity
from receptor_affinity.exceptions import RouteUnavailableError, RouteMismatchError
from receptor_affinity.mesh import Mesh, Node


def test_version():
    assert hasattr(receptor_affinity, '__version__')


def test_str_route_unavailable_error():
    """Call ``str`` on a ``RouteUnavailableError``."""
    node = Node('my node')
    err = RouteUnavailableError(node)
    str(err)


def test_str_route_mismatch_error():
    """Call ``str`` on a ``RouteMismatchError``."""
    # Node.__init__ doesn't require all required attributes. It should be fixed, but in the
    # meantime, using Node.create_from_config works around this issue, as it provides many default
    # values.
    node = Node.create_from_config({'name': 'mynode'})
    node.start()
    mesh = Mesh()
    mesh.add_node(node)
    err = RouteMismatchError(mesh, (node,))
    str(err)
