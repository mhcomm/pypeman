"""Tests for the structured channel representation of the graph plugin."""

import json

import pytest

from pypeman import channels
from pypeman import nodes
from pypeman.plugins.graph import build_channel_repr
from pypeman.plugins.graph import build_graph_repr
from pypeman.plugins.graph import render_ascii
from pypeman.plugins.graph import render_dot
from pypeman.plugins.graph import serialize_condition
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401 (fixture)


def is_urgent(msg):
    return True


@pytest.fixture
def main_channel(clear_graph):  # noqa: F811 (fixture use)
    """A channel exercising node/fork/when/case + special end nodes."""
    main = channels.BaseChannel(name="main")
    main.append(nodes.Log(name="entry"))
    main.fork(name="audit").append(nodes.Log(name="audit_log"))
    main.when(is_urgent, name="urgent").append(nodes.Log(name="alert"))
    hot, cold = main.case(is_urgent, True, names=["hot", "cold"])
    hot.append(nodes.Log(name="hot_log"))
    cold.append(nodes.Log(name="cold_log"))
    main.append(nodes.Log(name="save"))
    main.add_fail_nodes(nodes.Log(name="notify_fail"))
    main.add_final_nodes(nodes.Log(name="cleanup"))
    return main


def test_serialize_condition_callable():
    cond = serialize_condition(is_urgent)
    assert cond["callable"] is True
    assert cond["name"] == "is_urgent"
    assert cond["str"].endswith(".is_urgent")


def test_serialize_condition_value():
    cond = serialize_condition(True)
    assert cond == {"callable": False, "value": True, "str": "True"}


def test_build_channel_repr_structure(main_channel):
    repr_ = build_channel_repr(main_channel)

    assert repr_["kind"] == "channel"
    assert repr_["id"] == "main"
    assert repr_["class"] == "BaseChannel"
    assert repr_["short_name"] == "main"

    kinds = [step["kind"] for step in repr_["nodes"]]
    assert kinds == ["node", "fork", "when", "case", "node"]

    node, fork, when, case, _ = repr_["nodes"]
    assert node == {"kind": "node", "id": "main.entry", "class": "Log", "name": "entry"}

    assert fork["channel"]["id"] == "main.audit"
    assert fork["channel"]["class"] == "SubChannel"
    assert fork["channel"]["short_name"] == "audit"
    assert fork["channel"]["nodes"][0]["id"] == "main.audit.audit_log"

    assert when["condition"]["name"] == "is_urgent"
    assert when["channel"]["class"] == "ConditionSubChannel"

    assert case["id"] == "main.case_3"  # synthesized from the step position
    assert [b["channel"]["short_name"] for b in case["branches"]] == ["hot", "cold"]
    assert case["branches"][1]["condition"] == {"callable": False, "value": True, "str": "True"}

    assert set(repr_["specials"]) == {"fail", "final"}
    assert repr_["specials"]["final"][0]["name"] == "cleanup"


def test_build_graph_repr_is_json_able(main_channel):
    graph = build_graph_repr([main_channel])
    assert graph["version"] == 1
    round_tripped = json.loads(json.dumps(graph))
    assert round_tripped == graph


def test_render_ascii_main_path(main_channel, capsys):
    render_ascii(build_channel_repr(main_channel), [""], None)
    out = capsys.readouterr().out
    assert out == (
        "|-entry\n"
        "|=\\ (main.audit)\n"
        "|  |-audit_log\n"
        "|?\\ (main.urgent)\n"
        "|  |-alert\n"
        "|  -> out\n"
        "|c0\\\n"
        "|  |-hot_log\n"
        "|<--\n"
        "|c1\\\n"
        "|  |-cold_log\n"
        "|<--\n"
        "|-save\n"
    )


def test_render_ascii_special_final(main_channel, capsys):
    # used to crash on an `assert not "implemented"`
    render_ascii(build_channel_repr(main_channel), [""], "final")
    assert capsys.readouterr().out == "|-cleanup\n"


def test_render_dot_smoke(main_channel, capsys):
    first, last = render_dot(build_channel_repr(main_channel), [""], None)
    out = capsys.readouterr().out
    assert first == '"BaseChannel main"'
    assert last == '"Log save"'
    assert '"main.case_3.top" [shape=diamond label="case (2 branches)"]' in out
    assert '[style=dashed label="case True"]' in out

    # the special paths render without error too
    render_dot(build_channel_repr(main_channel), [""], "final")
