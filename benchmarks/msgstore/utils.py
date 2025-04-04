from __future__ import annotations
from datetime import datetime
from pypeman import msgstore
from pypeman.message import Message
import random as rnd

ALL_STORES_FACTORIES: dict[str, type[msgstore.MessageStoreFactory]] = {
    "fake": msgstore.FakeMessageStoreFactory,
    "file": msgstore.FileMessageStoreFactory,
    "memory": msgstore.MemoryMessageStoreFactory,
    "null": msgstore.NullMessageStoreFactory,
}


def random_meta():
    return {}


def random_payload():
    return "todo"


def random_message(min_max_dt: tuple[datetime, datetime] | None = None):
    r = Message(payload=random_payload(), meta=random_meta())
    if min_max_dt:
        r.payload = ...
        assert not "implemented"
    return r


def random_search(
    count_range: tuple[int, int] = (10, 100),
    min_max_dt: tuple[datetime, datetime] | None = None,
    ids: list[str] | None = None,
):
    assert not "implemented"
    return {
        "count": int,
        "order_by": Literal["timestamp", "status", "-timestamp", "-status"],
        "start_dt": datetime | None,
        "end_dt": datetime | None,
        "text": str | None,
        "rtext": str | None,
        "start_id": str | None,
        "meta": dict[str, str] | None,
    }
