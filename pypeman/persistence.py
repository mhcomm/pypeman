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
    """
    Return the configured backend instance.

    :param loop: Asyncio loop to use. Passed backend instance.
    """
    global _backend
    if not _backend:
        # Load backend on first use
        if not settings.PERSISTENCE_BACKEND:
            raise Exception("Persistence backend not configured.")

        module, _, class_ = settings.PERSISTENCE_BACKEND.rpartition('.')
        loaded_module = importlib.import_module(module)
        _backend = getattr(loaded_module, class_)(loop=loop, **settings.PERSISTENCE_CONFIG)

        await _backend.start()

    return _backend


class MemoryBackend():
    """
    Memory persistence backend.
    Only for testing purpose as this is not really persistent.
    """
    def __init__(self, loop=None):
        self.loop = loop
        self._data = defaultdict(dict)

    async def start(self):
        """
        Do nothing for this backend.
        """
        pass

    async def store(self, namespace, key, value):
        """ Store the value in a dict in memory

        :param namespace: Namespace for dict.
        :param key: Access key.
        :param value: Value to save.
        """
        self._data[namespace][key] = value

    async def get(self, namespace, key, default=SENTINEL):
        """
        Get the value from memory.

        :param namespace: Namespace for dict.
        :param key: Key to get.
        :param default: Default value if key missing.
        """
        if default is not SENTINEL:
            return self._data[namespace].get(key, default)
        else:
            return self._data[namespace][key]


class SqliteBackend():
    """
    Sqlite persistence backend. Store data in an sqlite database with
    ACID garanties. Internally use a thread pool to execute database access.

    :param path: Path of sqlite file.
    :param thread_pool: If you want a specific thread_pool you can give one here.
    :param loop: Loop used for the executor.
    """
    def __init__(self, path, thread_pool=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.executor = thread_pool or ThreadPoolExecutor(max_workers=1)
        self.path = path

    async def start(self):
        """
        Do nothing for this backend.
        """
        pass

    def _sync_store(self, namespace, key, value):
        with SqliteDict(self.path, tablename=namespace) as pdict:
            pdict[key] = value
            pdict.commit()

    def _sync_get(self, namespace, key, default):
        with SqliteDict(self.path, tablename=namespace) as pdict:
            if default is not SENTINEL:
                return pdict.get(key, default)
            else:
                return pdict[key]

    async def store(self, namespace, key, value):
        """ Store the value in a dict saved in sqlite db.

        :param namespace: Namespace table name.
        :param key: Access key.
        :param value: Value to save.
        """
        await self.loop.run_in_executor(self.executor, self._sync_store, namespace, key, value)

    async def get(self, namespace, key, default=SENTINEL):
        """
        Get the value from sqlite db.

        :param namespace: Namespace table name.
        :param key: Key to get.
        :param default: Default value if key missing.
        """
        return await self.loop.run_in_executor(self.executor, self._sync_get, namespace, key, default)
