from __future__ import annotations
from datetime import datetime
from datetime import timedelta
from pypeman import msgstore
from pypeman.message import Message
import random as rnd

ALL_STORES_FACTORIES: dict[str, type[msgstore.MessageStoreFactory]] = {
    "fake": msgstore.FakeMessageStoreFactory,
    "file": msgstore.FileMessageStoreFactory,
    "memory": msgstore.MemoryMessageStoreFactory,
    "null": msgstore.NullMessageStoreFactory,
}

FRUITS = [
    "apple",
    "banana",
    "cherry",
    "date",
    "elderberry",
    "fig",
    "grape",
    "honeydew",
    "kiwi",
    "lemon",
    "mango",
    "nectarine",
    "orange",
    "papaya",
    "quince",
    "raspberry",
    "strawberry",
    "tangerine",
    "ugli",
    "vanilla",
    "watermelon",
    "xigua",
    "yellowfruit",
    "zucchini",
]


def random_dt(min_dt: datetime, max_dt: datetime):
    """Random datetime between the two, min inclusive max exclusive."""
    seconds = rnd.random() * (max_dt - min_dt).total_seconds()
    return min_dt + timedelta(seconds=seconds)


def random_payload():
    """Random text of 10 to 50 random words (fruits)."""
    return " ".join(rnd.choice(FRUITS) for _ in range(rnd.randint(10, 50)))


def random_message(min_max_dt: tuple[datetime, datetime] | None = None):
    """Combines :func:`random_payload` and :func:`random_message`."""
    r = Message(payload=random_payload(), meta={})
    if min_max_dt:
        r.timestamp = random_dt(*min_max_dt)
    return r


def random_search(
    count_range: tuple[int] | tuple[int, int] = (10, 100),
    min_max_dt: tuple[datetime, datetime] | None = None,
    ids: list[str] | None = None,
):
    """Random search parameters.

    If `min_max_dt` is not given it will never have `start_dt`/`end_dt`
    otherwise it's a 50% individually.

    There is a 50% chance of there being a `text` param which will be
    a random word (fruit). No `rtext`.

    If `ids` is not given it will never have `start_id`
    otherwise it's a 50%.
    """
    r = {
        "count": rnd.randrange(*count_range),
        "order_by": rnd.choice(["timestamp", "state", "-timestamp", "-state"]),
    }

    if min_max_dt:
        a, b = random_dt(*min_max_dt), random_dt(*min_max_dt)
        a, b = (a, b) if a < b else (b, a)
        if rnd.random() < 0.5:
            r["start_dt"] = a
        if rnd.random() < 0.5:
            r["end_dt"] = b

    if rnd.random() < 0.5:
        r["text"] = rnd.choice(FRUITS)

    if ids and rnd.random() < 0.5:
        r["start_id"] = rnd.choice(ids)

    # TODO .. r['meta'] ..

    return r
