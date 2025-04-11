"""
This benchmark first ensures there are a lot of messages then does
random searches through these.

A previous run's directory can be re-used.
"""

from __future__ import annotations
import random as rnd

from pypeman.message import Message
from pypeman.msgstore import MessageStore
from utils import ALL_STORES_FACTORIES
from utils import FRUITS
from utils import random_message
from utils import random_search

factory = ALL_STORES_FACTORIES["memory"]()
N_MESSAGES = 10000
N_SEARCHES = 1000

# generate store-related meta as if done through the `BaseNode.store_meta`
N_STORE_META = (10, 100)  # how many meta to have per message
N_STORE_META_ENTRIES = (1, 5)  # for number of items in each list

MIN_MAX_DT = None


async def setup():
    store = factory.get_store("banana")
    await store.start()

    while await store.total() < N_MESSAGES:
        id = await store.store(random_message(min_max_dt=MIN_MAX_DT))
        # generate the store-related meta,
        # these are all lists as if done through the `BaseNode.store_meta`
        for _ in range(rnd.randrange(*N_STORE_META)):
            meta_info_name = rnd.choice(FRUITS)
            count = rnd.randrange(*N_STORE_META_ENTRIES)
            info_list = [str(rnd.randint(10, 99)) for _ in range(count)]
            await store.add_message_meta_infos(id, meta_info_name, info_list)

    ids = [res["id"] for res in await store.search(count=N_MESSAGES)]
    searches = (random_search(min_max_dt=MIN_MAX_DT, ids=ids) for _ in range(N_SEARCHES))

    return store, searches


async def run(store: MessageStore, searches: list):
    successes = []
    for it in searches:
        try:
            successes.append(len(await store.search(**it)))
        except LookupError:
            pass
    print(f"{len(successes)} sucesses ({N_SEARCHES - len(successes)} failures)")
    print(f"{sum(successes) / len(successes)} results on average")
