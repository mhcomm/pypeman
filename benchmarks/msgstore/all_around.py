"""
This benchmark tries to simulate a fairly balanced / actual usage
situation for a message store.
"""

from __future__ import annotations
import random as rnd

from pypeman.message import Message
from pypeman.msgstore import MessageStore
from utils import ALL_STORES_FACTORIES
from utils import random_payload

factory = ALL_STORES_FACTORIES['memory']()
N = 1000

OP_COUNTS = {
    "store": N,
    "get": N * 3,
    "get_message_meta_infos": N * 20,
    "add_message_meta_infos": N * 2,
    "change_message_state": N * 7,
    # "get_payload_str": N // 50,
    "search": N // 20,
}


class op_funcs:
    @staticmethod
    async def store(store: MessageStore, ids: list[str]):
        ids.append(await store.store(Message(payload=random_payload())))

    @staticmethod
    async def get(store: MessageStore, ids: list[str]):
        await store.get(rnd.choice(ids))

    @staticmethod
    async def get_message_meta_infos(store: MessageStore, ids: list[str]):
        await store.get_message_meta_infos(rnd.choice(ids), "state")

    @staticmethod
    async def add_message_meta_infos(store: MessageStore, ids: list[str]):
        await store.add_message_meta_infos(rnd.choice(ids), f"meta{rnd.random()}", rnd.randbytes(7))

    @staticmethod
    async def change_message_state(store: MessageStore, ids: list[str]):
        await store.change_message_state(rnd.choice(ids), rnd.choice(Message.STATES_PRIORITY))

    @staticmethod
    async def get_payload_str(store: MessageStore, ids: list[str]):
        "await store.get_payload_str(rnd.choice(ids))"

    @staticmethod
    async def search(store: MessageStore, ids: list[str]):
        await store.search(count=100, start_id=rnd.choice(ids))


async def setup():
    store = factory.get_store("banana")
    await store.start()

    todo_ops = [getattr(op_funcs, op) for op, count in OP_COUNTS.items() for _ in range(count)]
    rnd.shuffle(todo_ops)
    todo_ops.insert(0, op_funcs.store)  # ensure there's at least 1 message stored

    return store, todo_ops


async def run(store: MessageStore, todo_ops: list):
    ids = []
    print(f"total count: {len(todo_ops)}")
    for op in todo_ops:
        await op(store, ids)

# vim: se tw=101:
