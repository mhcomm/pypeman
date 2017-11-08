"""
This module contains all persistence related things.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import importlib

from sqlitedict import SqliteDict

from pypeman.conf import settings

SENTINEL = object()

_backend = None

async def get_backend(loop):
    global _backend

    if not _backend:

        if not settings.PERSISTENCE_BACKEND:
            raise Exception("Persistence backend not configured.")

        module, _, class_ = settings.PERSISTENCE_BACKEND.rpartition('.')
        loaded_module = importlib.import_module(module)
        _backend = getattr(loaded_module, class_)(loop=loop, **settings.PERSISTENCE_CONFIG)

    return _backend


class MemoryBackend():
    def __init__(self, loop=None):
        self._data = defaultdict(dict)

    async def start(self):
        pass

    async def store(self, namespace, key, value):
        self._data[namespace][key] = value

    async def get(self, namespace, key, default=SENTINEL):
        if default is not SENTINEL:
            return self._data[namespace].get(key, default)
        else:
            return self._data[namespace][key]


class SqliteBackend():
    def __init__(self, path, thread_pool=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

        if thread_pool is None:
            self.executor = ThreadPoolExecutor(max_workers=1)
        else:
            self.executor = thread_pool

        self.path = path

    async def start(self):
        pass

    def _sync_store(self, namespace, key, value):
        with SqliteDict(self.path, tablename=namespace) as pdict:
            pdict[key] = value

    def _sync_get(self, namespace, key, default):
        with SqliteDict(self.path, tablename=namespace) as pdict:
            if default is not SENTINEL:
                return pdict.get(key, default)
            else:
                return pdict[key]

    async def store(self, namespace, key, value):
        self.loop.run_in_executor(self.executor, self._sync_store, namespace, key, value)

    async def get(self, namespace, key, default=SENTINEL):
        return self.loop.run_in_executor(self.executor, self._sync_get, namespace, key, default)

