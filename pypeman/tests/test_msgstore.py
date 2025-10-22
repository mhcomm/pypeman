from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime
from datetime import timedelta
from re import error as ReError
from tempfile import TemporaryDirectory
from typing import Any
from typing import Literal

import pytest

from .. import msgstore
from ..message import Message
from ..msgstore import MessageStoreFactory

_NEEDS_TMPDIR = object()

TESTED_STORE_FACTORIES = [
    # can't be tested generically, breaks too many invariants
    # (msgstore.NullMessageStoreFactory, ()),
    (msgstore.MemoryMessageStoreFactory, ()),
    (msgstore.FileMessageStoreFactory, (_NEEDS_TMPDIR,)),
]

TESTED_PERSISTENT_STORE_FACTORIES = [
    (msgstore.FileMessageStoreFactory, (_NEEDS_TMPDIR,)),
]


@pytest.fixture(scope="function", params=TESTED_STORE_FACTORIES, ids=lambda p: p[0])
async def factory(request: pytest.FixtureRequest):
    "Fixture that, when used in a test function, will test every store."
    current: tuple[type[MessageStoreFactory], tuple[object, ...]] = request.param
    cls, args = current

    exst = ExitStack()
    args = tuple(exst.enter_context(TemporaryDirectory()) if a is _NEEDS_TMPDIR else a for a in args)

    factory = cls(*args)
    # this makes it possible to re-create a factory with the same arguments from within the test
    setattr(factory, "_re_init_new_tg__testtest", lambda: cls(*args))
    with exst:
        yield factory

        for store_id in factory.list_store():
            await factory.delete_store(store_id)


async def test_store_identity(factory: MessageStoreFactory):
    store_a = factory.get_store("a")
    store_b = factory.get_store("b")
    assert store_a is not store_b
    store_a_again = factory.get_store("a")
    assert store_a is store_a_again


async def test_store_retrieve(factory: MessageStoreFactory):
    """For a restored message, it ensures the following:
    * timestamp is a datetime and equal to original
    * uuid is a str and equal to original
    * payload
    * meta is a POPO (ie. json-compatible) dict
    * ctx is a plain dict of dicts with `'payload'` `'meta'`
    """
    store = factory.get_store("a")
    await store.start()

    msg = Message(
        payload="thingymabob",
        meta={
            "": "empty named meta",
            "meta that is a list": [1, 2, 3],
            "true": False,
            "queneni": None,
        },
    )
    id = await store.store(msg)
    assert type(id) is str

    entry = await store.get(id)
    assert entry["id"] == id

    remsg = entry["message"]
    assert remsg.timestamp == msg.timestamp
    assert remsg.uuid == msg.uuid
    assert remsg.payload == msg.payload
    assert remsg.meta == msg.meta
    assert remsg.ctx == msg.ctx


async def test_store_delete(factory: MessageStoreFactory):
    "Using an `id` after a call to :meth:`MessageStore.delete` fails."
    store = factory.get_store("a")
    await store.start()

    id = await store.store(Message())
    _ = await store.get(id)  # store successful, message exists
    await store.delete(id)

    # do it with both implementation methods directly because we want
    # to make sure both work (one might always shadow the other if eg the
    # meta is always used first in implementation of says `.get`)
    rose = None
    try:
        _ = await store._get_storemeta(id)
    except BaseException as exc:
        rose = exc
    assert isinstance(rose, LookupError), rose
    try:
        _ = await store._get_message(id)
    except BaseException as exc:
        rose = exc
    assert isinstance(rose, LookupError), rose


async def test_get_invalid(factory: MessageStoreFactory):
    "Using a wrong `id` fails."
    store = factory.get_store("a")
    await store.start()

    id = await store.store(Message())

    rose = None
    try:
        _ = await store._get_storemeta("nop!" + id)
    except BaseException as exc:
        rose = exc
    assert isinstance(rose, LookupError), rose
    try:
        _ = await store._get_message("nop!" + id)
    except BaseException as exc:
        rose = exc
    assert isinstance(rose, LookupError), rose


async def test_store_state_management(factory: MessageStoreFactory):
    "Updates to the state"
    store = factory.get_store("a")
    await store.start()

    id = await store.store(Message())
    assert await store.get_message_state(id) == Message.PENDING

    await store.change_message_state(id, Message.PROCESSING)
    assert await store.get_message_state(id) == Message.PROCESSING

    await store.change_message_state(id, Message.PROCESSED)
    assert await store.get_message_state(id) == Message.PROCESSED


async def test_double_store_message(factory: MessageStoreFactory):
    "By double store I mean twice the same, (same uuid)."
    store = factory.get_store("a")
    await store.start()

    msg = Message()
    _ = await store.store(msg)

    rose = None
    try:
        _ = await store.store(msg)
    except BaseException as exc:
        rose = exc
    assert isinstance(rose, ValueError), rose


@pytest.mark.parametrize("factory", TESTED_PERSISTENT_STORE_FACTORIES, indirect=True)
async def test_store_persistence(factory: MessageStoreFactory):
    "Store, shutdown, reboot, expect messages to still be there."
    store = factory.get_store("a")
    await store.start()

    ids = [await store.store(Message(payload=k)) for k in range(3)]

    # pypeman shutdown (only thing that matter is to re-new 'factory' with same params)
    del store

    # re-create a factory with the exact same argument
    factory = getattr(factory, "_re_init_new_tg__testtest")()
    store = factory.get_store("a")
    await store.start()

    assert await store.total() == 3
    for k, entry in enumerate(await store.get_many(ids)):
        assert entry["message"].payload == k


async def test_get_aliases_fake_coverage():
    """Things that might not be needed anymore but are kept for now:
    * get_preview_str
    * get_msg_content
    * get_payload_str
    * is_regex_in_msg
    * is_txt_in_msg
    """
    # same reasoning as for `search` tests; any store does it
    store = msgstore.MemoryMessageStoreFactory().get_store("a")
    await store.start()

    id = await store.store(Message(payload={"banana": ["good"]}))

    assert (await store.get_preview_str(id)).payload == str({"banana": ["good"]})
    assert (await store.get_msg_content(id)).payload == {"banana": ["good"]}
    assert await store.get_payload_str(id) == str({"banana": ["good"]})

    assert await store.is_regex_in_msg(id, "b[an]+")
    assert not await store.is_regex_in_msg(id, "0")
    rose = None
    try:
        assert await store.is_regex_in_msg(id, ":)")
    except BaseException as exc:
        rose = exc
    assert isinstance(rose, ReError), rose

    assert await store.is_txt_in_msg(id, "oo")
    assert not await store.is_txt_in_msg(id, "xx")


async def test_span_select_many(factory: MessageStoreFactory):
    """Test span selection:
        * :meth:`MessageStore._span_select`
        * :meth:`MessageStore.get_many`

    As part of testing the search, stores must be able to provide
    all the ids of message within a given time span. `get_many`
    might be overriden so it is tested alongside.

    Reminders:
        we guarentee to `_span_select` implementations that:
            * `start_dt < end_dt`;
            * `self.total() != 0`.
        we guarentee to `get_many` reimplementations that:
            * `1 < len(ids)`;
            * `self.total() != 0`.
    """
    store = factory.get_store("a")
    await store.start()

    # make a bunch of messages with sorted timestamps and non-sorted payloads
    msgs = [Message(payload=char) for char in "123546798"]
    msg = msgs[0]
    first_dt = last_dt = msg.timestamp
    for msg in msgs[1:]:
        last_dt += timedelta(1)
        msg.timestamp = last_dt
    del msg

    # store these but not in timestamp order (notice char in list above)
    not_ts_sorted_msgs = sorted(msgs, key=lambda msg: msg.payload)
    not_ts_sorted_ids = [await store.store(msg) for msg in not_ts_sorted_msgs]
    # re-sort the ids by corresponding message timestamp
    ids = [
        id
        for id, msg in sorted(
            zip(not_ts_sorted_ids, not_ts_sorted_msgs),
            key=lambda pair: pair[1].timestamp,
        )
    ]
    # now `msgs` and `ids` are in correspondence; this whole bit above is to test
    # the store even when messages may not have been stored in timestamp order

    async def tester(start_dt: datetime | None, end_dt: datetime | None, expect: slice):
        "a tester walks into a bar- runs into a bar- crawls into a bar-"
        sel = await store._span_select(start_dt, end_dt)
        # this tests for both correct ids and correct ordering
        assert sel == ids[expect]
        # if enough elements, also test `get_many`
        if 1 < len(sel):
            many = await store.get_many(sel)
            # (explicit loop, not with :func:`all` for better reporting)
            for expected, actual in zip(msgs[expect], many):
                # (only match the payload, rest is not this test's responsibility)
                assert actual["message"].payload == expected.payload

    # span largely wide enough to select everything
    await tester(first_dt - timedelta(5), last_dt + timedelta(5), slice(9))
    # span outside before / outside after
    await tester(first_dt - timedelta(10), first_dt - timedelta(1), slice(0))
    await tester(last_dt + timedelta(1), last_dt + timedelta(10), slice(0))
    # span with start/end only (hshift excludes the first/last 2)
    await tester(first_dt + timedelta(1.5), None, slice(2, 9))
    await tester(None, last_dt - timedelta(1.5), slice(-2))
    # span with neither start nor end (should be everything)
    await tester(None, None, slice(9))
    # result should be start inclusive and end exclusive
    await tester(msgs[2].timestamp, msgs[4].timestamp, slice(2, 4))


# for every search tests: the tested function is in base :class:`MessageStore`
# which is abstract; :meth:`search` is not to be overriden, so we can pick any
# (lightweight and easily cleaned) concrete implementing class


async def test_search_dum_checks():
    "Collection of not that bright tests for search."
    store = msgstore.MemoryMessageStoreFactory().get_store("a")
    await store.start()

    # empty store
    assert await store.search() == [], "search without group_by should be a list"
    assert await store.search(group_by="state") == {}, "search with group_by should be a dict"

    await store.store(Message(payload=0))

    # still supported for now (this test is not contractual)
    await store.search(start_dt="2002 12")
    await store.search(end_dt="2002-123 17:42")

    # broken time spans
    try:
        await store.search(start_dt=datetime(5555, 1, 1), end_dt=datetime(5555, 1, 1))
    except BaseException:
        pass
    else:
        raise AssertionError("search should have not accepted parameters (same dt)")
    try:
        await store.search(start_dt=datetime(8888, 1, 1), end_dt=datetime(2222, 1, 1))
    except BaseException:
        pass
    else:
        raise AssertionError("search should have not accepted parameters (backward)")


async def test_search_hashtag_nofilter():
    "Searches with no params are expected to return everything."
    store = msgstore.MemoryMessageStoreFactory().get_store("a")
    await store.start()

    list_ids = [await store.store(Message(payload=char)) for char in "123546798"]
    ids = set(list_ids)
    # sanity check that shouldn't be necessary; but this invarient is crucial for the rest of the test
    assert len(ids) == 9, "sanity check oops; cannot test"
    # change a few messages' state
    await store.change_message_state(list_ids[3], Message.REJECTED)
    await store.change_message_state(list_ids[7], Message.ERROR)

    found = await store.search()
    assert isinstance(found, list), "search without group_by should be a list"
    assert {entry["id"] for entry in found} == ids, found

    # (remark: this is not a comprehensive `group_by` test! this test is
    # only concerned with the fact that every message stored is returned;
    # this is why we are fine with not caring about the actual groups)
    found = await store.search(group_by="state")
    assert isinstance(found, dict), "search with group_by should be a dict"
    assert {entry["id"] for group in found.values() for entry in group} == ids, found


async def test_search_count_and_start_id():
    "Tests `count` and `start_id`."
    store = msgstore.MemoryMessageStoreFactory().get_store("a")
    await store.start()

    # make a bunch of messages with increasing timestamps (because we'll be ordering by timestamp)
    msgs = [Message(payload=char) for char in "123546798"]
    msg = msgs[0]
    last_dt = msg.timestamp
    for msg in msgs[1:]:
        last_dt += timedelta(1)
        msg.timestamp = last_dt
    del msg
    ids = [await store.store(msg) for msg in msgs]

    async def tester(
        count: int,
        start_id: str | None,
        direction: Literal["+", "-"],  # easier to read at call site than True/False
        expect_or_except: slice | type[BaseException],
    ):
        "a tester walks into a bar- runs into a bar- crawls into a bar-"
        try:
            found = await store.search(
                count=count,
                start_id=start_id,
                order_by="-timestamp" if "-" == direction else "timestamp",
            )

        except BaseException as rose:
            if isinstance(expect_or_except, slice):
                raise  # were not expecting an exception
            assert isinstance(rose, expect_or_except), expect_or_except
            return

        assert isinstance(expect_or_except, slice)
        assert isinstance(found, list)  # (at this point, this is mostly for type hints)
        expect = ids[expect_or_except]
        if "-" == direction:
            expect.reverse()
        assert [entry["id"] for entry in found] == expect, found

    # big enough of a count should get all of them
    await tester(42, None, "+", slice(9))
    # count of zero
    await tester(0, None, "+", slice(0))
    # smaller count should return begining (resp. ending) if non reversed (resp. reversed)
    await tester(4, None, "+", slice(4))  # .    [----     ]
    await tester(4, None, "-", slice(5, 9))  # . [     ----]
    # same idea but starting at a message
    await tester(3, ids[3], "+", slice(4, 7))  # [    |--- ]
    await tester(3, ids[4], "-", slice(1, 4))  # [ ---|    ]
    # same again but with less results than count
    await tester(3, ids[6], "+", slice(7, 9))  # [      |--]
    await tester(3, ids[2], "-", slice(0, 2))  # [--|      ]
    # 'out of range' start_id should throw
    # (we use a made-up id which for this function acts the same as an existing but out of range id)
    await tester(3, "$#!@", "+", ValueError)  # |[         ]


async def test_search_filter_order_group():
    "Test all of filtering/ordering/grouping"
    store = msgstore.MemoryMessageStoreFactory().get_store("a")
    await store.start()

    bunch: list[tuple[datetime, Any, dict[str, Any]]] = [
        (
            datetime(2000, 1, 1),
            "ayo",
            {"state": Message.PROCESSED, "one": "one", "two": ["ononn", "uouii"], "num": ["42"]},
        ),
        (
            datetime(2000, 1, 2),
            0,
            {"state": Message.ERROR, "one": ["not", "today", "!"], "two": ["okiuki", "-"], "num": ["15"]},
        ),
        (
            datetime(2000, 2, 1),
            True,
            {"state": Message.ERROR, "one": ["yesterday", "?", "one"], "num": ["12", "72"]},
        ),
        (
            datetime(2001, 1, 2),
            {9: 3 / 4},
            {"state": Message.REJECTED, "one": [], "num": ["notnum.. sad"]},
        ),
        (
            datetime(2001, 2, 1),
            ["payload", "is", "a", "list"],
            {"state": Message.PENDING, "one": [], "two": "ononn", "num": 37},
        ),
    ]
    # with other tests covering for edge cases, this one can be way less ceremonious
    ids: list[str] = []
    for timestamp, payload, meta in bunch:
        msg = Message(payload=payload)
        msg.timestamp = timestamp
        ids.append(await store._store(msg, meta))
    store._cached_total = len(ids)

    async def tester_filter(text: str | None, rtext: str | None, meta: dict[str, str], expect: set[int]):
        "a-"
        found = await store.search(text=text, rtext=rtext, meta=meta)
        assert isinstance(found, list)  # (at this point, this is mostly for type hints)
        assert {it["id"] for it in found} == {ids[k] for k in expect}, found

    async def tester_order(order_by: str, expect: list[int]):
        "tester-"
        found = await store.search(order_by=order_by)
        assert isinstance(found, list)  # (at this point, this is mostly for type hints)
        assert [it["id"] for it in found] == [ids[k] for k in expect], found

    async def tester_group(group_by: str, expect: dict[str, set[int]]):
        "walks-"
        found = await store.search(group_by=group_by)
        assert isinstance(found, dict)  # (at this point, this is mostly for type hints)
        ex = {g: {ids[k] for k in s} for g, s in expect.items()}
        assert {g: {it["id"] for it in l} for g, l in found.items()} == ex, found

    await tester_filter("ay", None, {}, {0, 4})
    await tester_filter(None, "\\d", {}, {1, 3})
    await tester_filter(None, None, {"exact_one": "one"}, {0, 2})
    await tester_filter(None, None, {"text_one": "day"}, {1, 2})
    await tester_filter(None, None, {"rtext_two": "(.).{2}\\1"}, {0, 1, 4})
    # this start/end range is inclusive (idk y i did that...)
    await tester_filter(None, None, {"start_num": "17"}, {0, 2, 4})
    await tester_filter(None, None, {"end_num": "17"}, {1, 2})
    # multiple filters combine with logical 'and'
    await tester_filter(",", None, {"exact_one": "one"}, set())
    await tester_filter(None, None, {"text_one": "day", "start_num": "22"}, {2}),
    # filter on multiple meta values (a list) is an implicit logical 'or';
    # specifying a range via start+end, with the message having ['12', '72']:
    # * what you might be expecting:
    #     12  [40   60]  72   -> not in
    # * what it's actually doing:
    #     12  [40   60   72   -> yes above
    #     12   40   60]  72   -> yes below
    #                           `> yes in
    await tester_filter(None, None, {"start_num": "40", "end_num": "60"}, {0, 2}),

    await tester_order("timestamp", [0, 1, 2, 3, 4])
    # not that these are not sorted by severity but alphabetically
    # (then by timestamp because stable sort)
    await tester_order("state", [1, 2, 4, 0, 3])
    await tester_order("meta_one", [3, 4, 1, 0, 2])
    await tester_order("-meta_one", [2, 0, 1, 4, 3])

    await tester_group(
        "state",
        {
            Message.ERROR: {1, 2},
            Message.PENDING: {4},
            Message.PROCESSED: {0},
            Message.REJECTED: {3},
        },
    )
    await tester_group(
        "meta_two",
        {
            "": {2, 3},
            "ononn": {0, 4},
            "okiuki": {1},
        },
    )
