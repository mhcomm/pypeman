import pytest
import wip as msgstore
from wip import MessageStoreFactory
from pypeman.message import Message

TESTED_STORE_FACTORIES = [
    # can't be tested generically, breaks too many invariants
    # (msgstore.NullMessageStoreFactory, ()),
    (msgstore.MemoryMessageStoreFactory, ()),
    (msgstore.FileMessageStoreFactory, ("/tmp/test/this/out",)),
]

TESTED_PERSISTENT_STORE_FACTORIES = [
    (msgstore.FileMessageStoreFactory, ("/tmp/test/this/out",)),
]


@pytest.fixture(scope="function", params=TESTED_STORE_FACTORIES, ids=lambda p: p[0])
async def factory(request):
    "Fixture that, when used in a test function, will test every store."
    current: ... = request.param
    current: tuple[type[MessageStoreFactory], tuple[object, ...]]

    cls, args = current
    factory = cls(*args)
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
    assert id == entry["id"]

    remsg = entry["message"]
    assert msg.timestamp == remsg.timestamp
    assert msg.uuid == remsg.uuid
    assert msg.payload == remsg.payload
    assert msg.meta == remsg.meta
    assert msg.ctx == remsg.ctx


async def test_store_delete(factory: MessageStoreFactory):
    "Using an `id` after a call to :meth:`MessageStore.delete` fails."
    store = factory.get_store("a")
    await store.start()

    id = await store.store(Message())
    _ = await store.get(id)
    await store.delete(id)

    rose = None
    try:
        _ = await store.get(id)
    except BaseException as exc:
        rose = exc
    assert isinstance(rose, LookupError), rose


async def test_store_state_management(factory: MessageStoreFactory):
    "Updates to the state"
    store = factory.get_store("a")
    await store.start()

    id = await store.store(Message())
    assert Message.PENDING == await store.get_message_state(id)

    await store.change_message_state(id, Message.PROCESSING)
    assert Message.PROCESSING == await store.get_message_state(id)

    await store.change_message_state(id, Message.PROCESSED)
    assert Message.PROCESSED == await store.get_message_state(id)


@pytest.mark.parametrize("cls,params", TESTED_PERSISTENT_STORE_FACTORIES)
async def test_store_persistence(cls: type[MessageStoreFactory], params: ...):
    factory = cls(*params)
    store = factory.get_store("a")
    await store.start()

    ids = [await store.store(Message(payload=k)) for k in range(3)]

    # pypeman shutdown (only thing that matter is to re-new 'factory' with same params)
    del store, factory

    factory = cls(*params)
    store = factory.get_store("a")
    await store.start()

    assert 3 == await store.total()
    for k, entry in enumerate(await store.get_many(ids)):
        assert k == entry["message"].payload

    for store_id in factory.list_store():
        await factory.delete_store(store_id)


# TODO:
#   these are not even implemented yet
#     * add_sub_message_state
#     * set_state_to_worst_sub_state
#   these might not be needed anymore but will be kept for now:
#     * get_preview_str
#     * get_msg_content
#     * get_payload_str
#     * is_regex_in_msg
#     * is_txt_in_msg


async def test_search(factory: MessageStoreFactory):
    store = factory.get_store("a")
    await store.start()

    ..., store

    assert not "implemented"
