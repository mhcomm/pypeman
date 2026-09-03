"""Provides :class:`GraphPlugin`.

The `graph` command first builds a structured representation of the
channels (see :func:`build_graph_repr`) then renders it as ascii art,
graphviz dot or plain JSON.

The structured representation is a JSON-able tree of dicts. Channels
are objects with an ordered `"nodes"` list of steps; each step has a
`"kind"`:
    * `"node"` -- a plain processing node;
    * `"fork"` -- a :class:`SubChannel`: the nested channel processes
        a copy of the message in parallel, the main path continues;
    * `"when"` -- a :class:`ConditionSubChannel`: if the condition
        passes, the nested channel replaces the rest of the main path;
    * `"case"` -- a :class:`Case`: the first branch whose condition
        matches runs, then the main path continues.
The special end-node paths (init/join/drop/reject/fail/final) appear
under the channel's `"specials"` (only the non-empty ones).
"""

import json
from argparse import SUPPRESS
from argparse import ArgumentParser
from argparse import Namespace
from typing import Iterable
from typing import Literal

from pypeman.channels import BaseChannel
from pypeman.channels import Case
from pypeman.channels import ConditionSubChannel
from pypeman.channels import SubChannel
from pypeman.channels import all_channels
from pypeman.channels import get_channel
from pypeman.nodes import BaseNode
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import CommandPluginMixin
from pypeman.project import load_project

SPECIAL_PATHS = ("init", "join", "drop", "reject", "fail", "final")

SpecialPath_ = Literal["init", "join", "drop", "reject", "fail", "final"]


class GraphPlugin(BasePlugin, CommandPluginMixin):
    """Provides the `graph` command."""

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument(
            "--format",
            "-f",
            choices=("ascii", "dot", "json"),
            default="ascii",
            help="output format (default: ascii art)",
        )
        parser.add_argument(
            # deprecated alias of `--format dot`
            "--dot",
            action="store_const",
            const="dot",
            dest="format",
            help=SUPPRESS,
        )
        parser.add_argument(
            "--special",
            "-s",
            nargs="?",
            choices=SPECIAL_PATHS,
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
            chan = get_channel(options.channel)
            if chan is None:
                raise SystemExit(f"error: no channel named {options.channel!r}")
            tips = [chan]
        else:
            tips = [chan for chan in all_channels if not chan.parent]

        graph = build_graph_repr(tips)

        if "json" == options.format:
            print(json.dumps(graph, indent=2))

        elif "dot" == options.format:
            print("digraph {")
            print("    node [shape=rect]")
            for chan_repr in graph["channels"]:
                _, last = render_dot(chan_repr, ["    "], options.special)
                out = f'"_{chan_repr["id"]}"'
                print(f'    {out} [shape=circle label=""]')
                print(f"    {last} -> {out}")
            print("}")

        else:
            for chan_repr in graph["channels"]:
                print(chan_repr["class"])
                render_ascii(chan_repr, [""], options.special)
                print("|-> out")
                print("")


def serialize_condition(cond: object) -> dict:
    """Serialize a `when`/`case` condition (callable or plain value).

    The `"str"` field is a short human-readable form used as label by
    the ascii/dot renderers (and later mermaid).
    """
    if callable(cond):
        module = getattr(cond, "__module__", None) or "?"
        name = getattr(cond, "__name__", None) or repr(cond)
        return {"callable": True, "module": module, "name": name, "str": f"{module}.{name}"}

    value = cond if isinstance(cond, (bool, int, float, str, type(None))) else repr(cond)
    return {"callable": False, "value": value, "str": str(cond)}


def _node_step(node: BaseNode) -> dict:
    return {
        "kind": "node",
        "id": node.fullpath() if node.channel is not None else str(node.name),
        "class": type(node).__name__,
        "name": str(node.name),
    }


def build_channel_repr(chan: BaseChannel) -> dict:
    """Build the structured representation of one channel.

    Walks `chan._nodes` (never `next_node`, which is only chained once
    the channel starts) and the special end-node lists.
    """
    steps = []
    for index, node in enumerate(chan._nodes):

        if isinstance(node, SubChannel):
            steps.append({"kind": "fork", "channel": build_channel_repr(node)})

        elif isinstance(node, ConditionSubChannel):
            steps.append({
                "kind": "when",
                "condition": serialize_condition(node.condition),
                "channel": build_channel_repr(node),
            })

        elif isinstance(node, Case):
            steps.append({
                "kind": "case",
                # `Case` has no name of its own: synthesize a stable id
                # from the owning channel and the step position
                "id": f"{chan.name}.case_{index}",
                "branches": [
                    {"condition": serialize_condition(cond), "channel": build_channel_repr(sub_chan)}
                    for cond, sub_chan in node.cases
                ],
            })

        else:
            steps.append(_node_step(node))

    specials = {}
    for path in SPECIAL_PATHS:
        end_nodes = getattr(chan, f"{path}_nodes")
        if end_nodes:
            specials[path] = [_node_step(node) for node in end_nodes]

    return {
        "kind": "channel",
        "id": str(chan.name),
        "class": type(chan).__name__,
        "name": str(chan.name),
        "short_name": str(chan.short_name),
        "nodes": steps,
        "specials": specials,
    }


def build_graph_repr(channels: Iterable[BaseChannel]) -> dict:
    """Build the whole structured representation, see module doc."""
    return {"version": 1, "channels": [build_channel_repr(chan) for chan in channels]}


def _select_steps(chan_repr: dict, special: SpecialPath_ | None) -> list[dict]:
    """Steps to render: one special path, or init + main + join."""
    specials = chan_repr["specials"]
    if special:
        return specials.get(special, [])
    return specials.get("init", []) + chan_repr["nodes"] + specials.get("join", [])


def render_ascii(chan_repr: dict, indent: list[str], special: SpecialPath_ | None):
    """Print a channel representation as ascii art."""
    prefix = "".join(indent)
    for step in _select_steps(chan_repr, special):
        kind = step["kind"]

        if "fork" == kind:
            print(f"{prefix}|=\\ ({step['channel']['name']})")
            indent.append("|  ")
            render_ascii(step["channel"], indent, special)
            indent.pop()

        elif "when" == kind:
            print(f"{prefix}|?\\ ({step['channel']['name']})")
            indent.append("|  ")
            render_ascii(step["channel"], indent, special)
            indent.pop()
            print(f"{prefix}|  -> out")

        elif "case" == kind:
            for i, branch in enumerate(step["branches"]):
                print(f"{prefix}|c{i}\\")
                indent.append("|  ")
                render_ascii(branch["channel"], indent, special)
                indent.pop()
                print(f"{prefix}|<--")

        else:
            print(f"{prefix}|-{step['name']}")


def render_dot(chan_repr: dict, indent: list[str], special: SpecialPath_ | None) -> tuple[str, str]:
    """Print the graphviz dot subgraph for a channel representation.

    The subgraph consists of all the rendered steps as a chain:
        * `"fork"` creates a nested subgraph, the forked channel is
            linked with dashes and the fake node is a double octagon;
        * `"when"` also creates a nested subgraph, conditional path
            linked with dashes, fake node shown as a diamond;
        * `"case"` also a diamond, the in and out of branches are
            dashed.

    :return: the (first, last) dot identifiers of the chain.
    """

    def emit(*a: object):
        """prints with current indentation level"""
        print("".join(indent), *a, sep="")

    def ident(repr_dict: dict) -> str:
        """make a dot identifier from a channel or node step"""
        ty = repr_dict["class"]
        nm = repr_dict["name"]
        return nm if nm.startswith(ty) else f'"{ty} {nm}"'

    emit("{")
    indent.append("    ")

    first = prev = ident(chan_repr)
    emit(prev, " [shape=ellipse]")
    for step in _select_steps(chan_repr, special):
        kind = step["kind"]

        if "fork" == kind:
            curr = ident(step["channel"])
            emit(curr, ' [shape=doubleoctagon label="fork"]')
            emit(prev, " -> ", curr)
            into, _ = render_dot(step["channel"], indent, special)
            emit(curr, " -> ", into, ' [style=dashed label="ran task"]')

        elif "when" == kind:
            curr = ident(step["channel"])
            emit(curr, f' [shape=diamond label="when ({step["condition"]["str"]})"]')
            emit(prev, " -> ", curr)
            into, _ = render_dot(step["channel"], indent, special)
            emit(curr, " -> ", into, ' [style=dashed label="if passes"]')

        elif "case" == kind:
            curr = f'"{step["id"]}.top"'
            join = f'"{step["id"]}.bot"'
            emit(curr, f' [shape=diamond label="case ({len(step["branches"])} branches)"]')
            emit(join, ' [shape=point label=""]')
            emit(prev, " -> ", curr)
            emit("{")
            indent.append("    ")
            for branch in step["branches"]:
                into, butt = render_dot(branch["channel"], indent, special)
                emit(curr, " -> ", into, f' [style=dashed label="case {branch["condition"]["str"]}"]')
                emit(butt, " -> ", join, " [style=dashed]")
            indent.pop()
            emit("}")
            emit(prev, " -> ", join)
            curr = join

        else:
            curr = ident(step)
            emit(prev, " -> ", curr)

        prev = curr
    last = prev

    indent.pop()
    emit("}")

    return first, last
