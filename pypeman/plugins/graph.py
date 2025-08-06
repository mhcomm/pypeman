"""Provides :class:`GraphPlugin`."""

from __future__ import annotations

from argparse import ArgumentParser
from argparse import Namespace
from typing import Literal

from ..channels import BaseChannel
from ..channels import Case
from ..channels import ConditionSubChannel
from ..channels import SubChannel
from ..channels import all_channels
from ..channels import get_channel
from ..commands import load_project
from ..nodes import BaseNode
from .base import BasePlugin
from .base import CommandPluginMixin


class GraphPlugin(BasePlugin, CommandPluginMixin):
    """Provides the `graph` command."""

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument(
            "--dot",
            action="store_true",
            help="generate output in graphviz 'dot' format instead of the default ascii-based",
        )
        parser.add_argument(
            "--special",
            "-s",
            nargs="?",
            choices={"init", "join", "drop", "reject", "fail", "final"},
            help="graph specifically this special node path, instead of the main one",
        )
        parser.add_argument(
            "channel",
            nargs="?",
            help="graph specifically this channel, regardless of whether it top-level or not",
        )

    async def command(self, options: Namespace):
        load_project()

        if options.channel:
            tips = [get_channel(options.channel)]
        else:
            tips = (chan for chan in all_channels if not chan.parent)

        if options.dot:
            print("digraph {")
            print("    node [shape=rect]")
            for chan in tips:
                _, last = _graph_gvdot(chan, ["    "], options.special)
                print(f'    _{id(chan)} [shape=circle label=""]')
                print(f"    {last} -> _{id(chan)}")
            print("}")
        else:
            for chan in tips:
                print(type(chan).__name__)
                _graph_ascii(chan, [""], options.special)
                print("|-> out")
                print("")


# XXX: format basically untouched for now
def _graph_ascii(
    chan: BaseChannel,
    indent: list[str],
    path: Literal["init", "join", "drop", "reject", "fail", "final"] | None,
):
    nodes = {
        "_": (chan.init_nodes or []) + chan._nodes + (chan.join_nodes or []),
        "init": chan.init_nodes,
        "join": chan.join_nodes,
        "drop": chan.drop_nodes,
        "reject": chan.reject_nodes,
        "fail": chan.fail_nodes,
        "final": chan.final_nodes,
    }[path or "_"] or []

    prefix = "".join(indent)
    for node in nodes:

        if isinstance(node, SubChannel):
            print(f"{prefix}|=\\ ({node.name})")
            indent.append("|  ")
            _graph_ascii(node, indent, path)
            indent.pop()

        elif isinstance(node, ConditionSubChannel):
            print(f"{prefix}|?\\ ({node.name})")
            indent.append("|  ")
            _graph_ascii(node, indent, path)
            indent.pop()
            print(f"{prefix}|  -> out")

        elif isinstance(node, Case):
            for i, c in enumerate(node.cases):
                print(f"{prefix}|c{i}\\")
                indent.append("|  ")
                _graph_ascii(c[1], indent, path)
                indent.pop()
                print(f"{prefix}|<--")

        else:
            print(f"{prefix}|-{node.name}")

    if path and chan.final_nodes:  # TODO: always?
        assert not "implemented"


def _graph_gvdot(
    chan: BaseChannel,
    indent: list[str],
    path: Literal["init", "join", "drop", "reject", "fail", "final"] | None,
) -> tuple[str, str]:
    """Make the subgraph for a channel.

    The subgraph will consiste of all the nodes in the main path as
    a chain. Intermittent channels are handled as followed:
        * :class:`SubChannel` creates a nested subgraph, the forked
            channel is linked with dashes and the fake node is a double
            octagon;
        * :class:`ConditionSubChannel` also creates a nested subgraph,
            conditional path liked with dashs, fake node shown as
            a diamond;
        * :class:`Case` also a diamond, the in and out of branches are
            dashed.
    """
    nodes = {
        "_": (chan.init_nodes or []) + chan._nodes + (chan.join_nodes or []),
        "init": chan.init_nodes,
        "join": chan.join_nodes,
        "drop": chan.drop_nodes,
        "reject": chan.reject_nodes,
        "fail": chan.fail_nodes,
        "final": chan.final_nodes,
    }[path or "_"] or []

    def fart(*a: object):
        """prints with current indentation level"""
        print("".join(indent), *a, sep="")

    def ident(b: BaseChannel | BaseNode):
        """make identifier from thing with `.name` (channel or node)"""
        ty = type(b)
        nm = str(b.name)
        return nm if nm.startswith(ty.__name__) else f'"{ty.__name__} {nm}"'

    def condit(c: type[bool] | bool):
        return f"{c.__module__} {c.__name__}" if callable(c) else str(c)

    fart("{")
    indent.append("    ")

    first = prev = ident(chan)
    fart(prev, " [shape=ellipse]")
    for node in nodes:

        if isinstance(node, SubChannel):
            curr = ident(node)
            fart(curr, ' [shape=doubleoctagon label="fork"]')
            fart(prev, " -> ", curr)
            # XXX: won't `node` (the channel) appear twice?
            into, _ = _graph_gvdot(node, indent, path)
            fart(curr, " -> ", into, ' [style=dashed label="ran task"]')

        elif isinstance(node, ConditionSubChannel):
            curr = ident(node)
            fart(curr, f' [shape=diamond label="when ({condit(node.condition)})"]')
            fart(prev, " -> ", curr)
            # XXX: won't `node` (the channel) appear twice?
            into, _ = _graph_gvdot(node, indent, path)
            fart(curr, " -> ", into, ' [style=dashed label="if passes"]')

        elif isinstance(node, Case):
            curr = f"top_{id(node)}"
            join = f"bot_{id(node)}"
            fart(curr, f' [shape=diamond label="case ({len(node.cases)} branches)"]')
            fart(join, ' [shape=point label=""]')
            fart(prev, " -> ", curr)
            fart("{")
            indent.append("    ")
            for cond, chan in node.cases:
                into, butt = _graph_gvdot(chan, indent, path)
                fart(curr, " -> ", into, f' [style=dashed label="case {condit(cond)}"]')
                fart(butt, " -> ", join, " [style=dashed]")
            indent.pop()
            fart("}")
            fart(prev, " -> ", join)
            curr = join

        else:
            curr = ident(node)
            fart(prev, " -> ", curr)

        prev = curr
    last = prev

    if path and chan.final_nodes:  # TODO: always?
        assert not "implemented"

    indent.pop()
    fart("}")

    return first, last
