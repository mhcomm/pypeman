"""
This benchmark ensures there are a lot of messages then does random
searches through these.

A previous run's directory can be re-used.
"""

from __future__ import annotations
import random as rnd

from pypeman.message import Message
from pypeman.msgstore import MessageStore
from utils import ALL_STORES_FACTORIES
from utils import random_message
from utils import random_search

factory = ALL_STORES_FACTORIES["memory"]()
N_MESSAGES = 1000
N_SEARCHES = 100

MIN_MAX_DT = None


async def setup():
    store = factory.get_store("banana")
    await store.start()

    while await store.total() < N_MESSAGES:
        await store.store(random_message(min_max_dt=MIN_MAX_DT))

    ids = [res["id"] for res in await store.search(count=N_MESSAGES)]
    searches = (random_search(min_max_dt=MIN_MAX_DT, ids=ids) for _ in range(N_SEARCHES))

    return store, searches


async def run(store: MessageStore, searches: list):
    for it in searches:
        await store.search(*it)


# vim: se tw=101:
