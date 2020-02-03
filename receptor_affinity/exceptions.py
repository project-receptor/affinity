"""Custom exceptions."""
import io


class BaseError(Exception):
    """Base class for all exceptions defined by this library."""


class RouteUnavailableError(BaseError):
    """Routes were requested from an inactive node."""

    def __init__(self, node):
        self._node_name = node.name

    def __str__(self):
        return f"Routes were requested from an inactive node, {self._node_name}"


class RouteMismatchError(BaseError):
    """A mesh and its nodes have differing routing tables."""

    def __init__(self, mesh, nodes):
        """Store the mesh and all nodes which have mis-matched routes."""
        self._mesh = mesh
        self._nodes = nodes

    def __str__(self):
        with io.StringIO() as msg:
            msg.write("A mesh and its nodes have differing routing tables. Mesh routes are listed ")
            msg.write("below. Mis-matched nodes are also listed, but not their routes, as doing ")
            msg.write("so requires live a /metrics URL on each node, which may not be available.\n")
            msg.write("\n")
            msg.write(f"Routes for mesh: {self._mesh.generate_routes()}\n")
            msg.write("\n")
            msg.write(f"Mismatched nodes: {', '.join(node.name for node in self._nodes)}")
            return msg.getvalue()
