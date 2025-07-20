from argparse import ArgumentParser
from argparse import Namespace

from .base import BasePlugin
from .base import CommandPluginMixin
from ..graph import load_project


class GraphPlugin(BasePlugin, CommandPluginMixin):
    """Provides the `graph` command."""

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument(
            "--dot",
            action="store_true",
            help="generate output in graphviz 'dot' format",
        )

    async def command(self, options: Namespace):
        load_project()

        assert not "implemented"




# grabbed from graph
from pypeman import channels


def mk_ascii_graph(title=None):
    """ Show pypeman graph as ascii output.
        Better reuse for debugging or new code
    """
    if title:
        yield title
    for channel in channels.all_channels:
        if not channel.parent:
            yield channel.__class__.__name__
            for entry in channel.graph():
                yield entry
            yield "|-> out"
            yield ""


def mk_graph(dot=False):
    if dot:
        yield "digraph testgraph{"

        # Handle channel node shape
        for channel in channels.all_channels:
            yield '{node[shape=box]; "%s"; }' % channel.name

        # Draw each graph
        for channel in channels.all_channels:
            if not channel.parent:
                for line in channel.graph_dot():
                    yield line

        yield "}"
    else:
        for line in mk_ascii_graph():
            yield line
