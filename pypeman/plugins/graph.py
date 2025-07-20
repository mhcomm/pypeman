from argparse import ArgumentParser
from argparse import Namespace

from .base import BasePlugin
from .base import CommandPluginMixin
from ..channels import BaseChannel
from ..channels import all_channels
from ..graph import load_project

from ..channels import SubChannel
from ..channels import ConditionSubChannel
from ..channels import Case


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

        tips = (chan for chan in all_channels if not chan.parent)

        print("digraph {")
        print("    node [shape=rect]")
        for chan in tips:
            graph_channel(chan, ["    "])
        print("}")


def ident(ty: type, nm: str):
    return nm if nm.startswith(ty.__name__) else f'"{ty.__name__} {nm}"'


def graph_channel(chan: BaseChannel, indent: list[str]) -> tuple[str, str]:
    def fart(*a: object):
        print("".join(indent), *a, sep="")

    fart("{")
    indent.append("    ")

    first = prev = ident(type(chan), chan.name)
    fart(prev)
    for node in chan._nodes:
        if isinstance(node, SubChannel):
            fart("// ", ident(type(node), node))
            into, curr = graph_channel(node, indent)
            fart(prev, " -> ", into, ' [label="parallel"]')
        elif isinstance(node, ConditionSubChannel):
            fart("// ", ident(type(node), node))
            into, curr = graph_channel(node, indent)
            fart(prev, " -> ", into, ' [label="condition"]')
        elif isinstance(node, Case):
            fart(f"{{ // annonymous 'Case' object ({len(node.cases)} branches)")
            indent.append("    ")
            for cond, chan in node.cases:
                into, curr = graph_channel(chan, indent)
                fart(prev, " -> ", into, f' [label="case {cond.__module__} {cond.__name__}"]')
            indent.pop()
            fart("}")
            curr = prev  # yes lol
        else:
            curr = ident(type(node), node.name)
            fart(prev, " -> ", curr)
        prev = curr
    last = prev

    indent.pop()
    fart("}")

    return first, last


# grabbed from graph
from pypeman import channels


def mk_ascii_graph(title=None):
    """Show pypeman graph as ascii output.
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
