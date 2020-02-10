"""Custom exceptions."""
import io


class BaseError(Exception):
    """Base class for all exceptions defined by this library."""


class RouteMismatchError(BaseError):
    """A mesh and its nodes have differing routing tables."""

    def __init__(self, mesh, nodes):
        """Store the mesh and all nodes which have mis-matched routes."""
        self._mesh = mesh
        self._nodes = nodes

    def __str__(self):
        with io.StringIO() as msg:
            msg.write(
                "A mesh and its nodes have differing routing tables. Mesh routes are listed "
            )
            msg.write(
                "below. Mis-matched nodes are also listed, but not their routes, as doing "
            )
            msg.write(
                "so requires live a /metrics URL on each node, which may not be available.\n"
            )
            msg.write("\n")
            msg.write(f"Routes for mesh: {self._mesh.generate_routes()}\n")
            msg.write("\n")
            msg.write(
                f"Mismatched nodes: {', '.join(node.name for node in self._nodes)}"
            )
            return msg.getvalue()


class NodeUnavailableError(BaseError):
    """An operation was performed on an unavailable node.

    An "unavailable node" is a ``Node`` object that doesn't have a corresponding receptor node
    process. This exception doesn't specify why the receptor process is unavailable. For example, it
    might have been cleanly stopped or it might have crashed.
    """
