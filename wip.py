from __future__ import annotations
from abc import ABC
from abc import abstractmethod
from copy import deepcopy
from datetime import datetime
from pathlib import Path, PurePath
from typing import Any
from typing import Literal
from typing import TypedDict
import asyncio
import os
import re


class PypemanConfigError(BaseException):
    pass


class Message:
    State_ = Literal["wait_retry", "pending", "processing", "processed", "rejected", "error"]
    timestamp: datetime
    uuid: str
    payload: Any

    def to_dict(self, encode_payload: bool = True) -> dict[str, Any]: ...
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Message: ...


# base classes {{{1
class MessageStoreFactory(ABC):
    """A :class:`MessageStoreFactory` instance can generate related
    :class:`MessageStore` instances for a specific `store_id`. The
    later in turn is needed to store :class:`Message`s.

    :abstract:
    """

    def __init__(self):
        self._stores: dict[str, MessageStore] = {}

    @abstractmethod
    def _new_store(self, store_id: str) -> MessageStore:
        """(implementation) Create a new store refered to as `store_id`.

        Note for implementing classes: this function doesn't perform
        any heavy work yet (not async), instead the returned store's
        :meth:`MessageStore.start` does.

        :param store_id: Identifier of the corresponding message store.
        :return: New :class:`MessageStore` instance.
        """

    def get_store(self, store_id: str) -> MessageStore:
        """Return a store corresponding to the given `store_id`.

        :param store_id: Identifier of the corresponding message store.
        :return: New or existing :class:`MessageStore` instance.
        """
        existing = self._stores.get(store_id)
        return existing or self._stores.setdefault(store_id, self._new_store(store_id))


class MessageStore(ABC):
    """A :class:`MessageStore` keep an history of processed messages.

    Stored messages are associated a unique `id`. The storage mechanism
    is implementation defined but enables retrieving the object
    :class:`Message` as it was stored.

    `id`s *must* be considered opaque types. They are guaranteed to be
    :class:`str` for ease of manipulation, but content *should not*
    be interpreted.

    Alongside each message is a store-related meta dict. (It is not
    related to the message's actual meta dict.)

    It provides the following main methods:
        * :meth:`start`
        * :meth:`store`
        * :meth:`delete`
        * :meth:`total`
        * :meth:`get`
        * :meth:`search`

    A valid deriving class must at least provide implementation for:
        * :meth:`_store`
        * :meth:`_delete`
        * :meth:`_total`
        * :meth:`_get_message`
        * :meth:`_get_storemeta`
        * :meth:`_set_storemeta`
        * :meth:`_span_select`

    It may also override:
        * :meth:`_start` -- default is no-op
        * :meth:`get_many` -- default might be inefficient

    :abstract:
    """

    class StoredEntry_(TypedDict):
        """Data format used when retrieving messages."""

        id: str  # store-dependant id, different from message.id
        meta: dict[str, Any]  # store-related meta, different from message.meta
        message: Message
        state: Message.State_  # TODO: remove in favor of _['meta']['state']
        # timestamp: datetime  # TODO: remove in favor of message.timestamp if equivalent

    async def _start(self):
        """(implementation) Called at startup to initialize the store.

        This method is not marked as abstract and thus not required.
        """

    @abstractmethod
    async def _store(self, msg: Message) -> str:
        """(implementation) Store a message.

        Implementation must ensure that all operations with an id
        obtained this way are valid until a matching :meth:`_delete`.

        :param msg: Message to be stored.
        :return: Store-dependant id.
        """

    @abstractmethod
    async def _delete(self, id: str):
        """(implementation) Delete a stored message.

        After this operation, the id must be considered invalid.
        The implementation is free to reuse ids.

        :param id: Store-dependant id.
        :raise LookupError: When the id does not name a stored message.
        """

    @abstractmethod
    async def _total(self) -> int:
        """(implementation) Compute, without caching, the number of
        messages stored at this time.

        :return: Total Stored message count.
        """

    @abstractmethod
    async def _get_message(self, id: str) -> Message:
        """(implementation) Retrieve a stored message.

        :param id: Store-dependant id.
        :return: Message as it was stored.
        :raise LookupError: When the id does not name a stored message.
        """

    @abstractmethod
    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        """(implementation) Retrieve the store-related meta.

        Note to implementing classes: this operation is considered
        cheaper than :meth:`_get_message`.

        :param id: Store-dependant id.
        :return: Store-related meta dict.
        :raise LookupError: When the id does not name a stored message.
        """

    @abstractmethod
    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        """(implementation) Replace the store-related meta.

        :param id: Store-dependant id.
        :return: Message as it was stored.
        :raise LookupError: When the id does not name a stored message.
        """

    @abstractmethod
    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        """(implementation) Gather the ids for messages that fall
        within the given time frame.

        To implementing classe: `start_dt` < `end_dt` is verified.

        :param start_dt: (optional) Inclusive lower bound, or None.
        :param end_dt: (optional) Exclusive upper bound, or None.
        """

    async def start(self):
        """Called at startup to initialize the store."""
        await self._start()
        self._cached_total = await self._total()

    async def store(self, msg: Message) -> str:
        """Store a message in the store.

        :param msg: Message to store.
        :return: Id for this specific message.
        """
        id = await self._store(msg)
        self._cached_total += 1
        return id

    async def delete(self, id: str) -> StoredEntry_:
        """Delete a message in the store corresponding to the given id.

        Useful for tests and (maybe in the future) for cleanup.
        !CAUTION! this cannot be undone.

        :param id: Store-dependant id.
        :return: Same as :meth:`get`. This may be removed.
        :raise LookupError: When the id does not name a stored message.
        """
        # TODO: don't, unnecessary
        entry = await self.get(id)
        await self._delete(id)
        self._cached_total -= 1
        return entry

    # TODO: this is not async anymore, marking it non-async shows it is a cheap operation
    async def total(self) -> int:
        """Number of messages stored at this time.

        :return: Total Stored message count.
        """
        return self._cached_total

    async def get(self, id: str) -> StoredEntry_:
        """Retrieve a store entry.

        :param id: Store-dependant id.
        :return: Message store entry, with the message as it was
            stored and a store-related meta dict.
        :raise LookupError: When the id does not name a stored message.
        """
        meta = await self._get_storemeta(id)
        return {
            "id": id,
            "meta": meta,
            "message": await self._get_message(id),
            "state": meta["state"],  # TODO: phase out/remove
        }

    async def get_many(self, ids: list[str]) -> list[StoredEntry_]:
        """Retrieve multiple store entries at once.

        Some implementation may override this method to be more
        efficient than repeatedly calling :meth:`get` in a loop.
        (Which is more or less what this default implementation does.)

        :param ids: Sequence of store-dependant id.
        :return: The sequence of store entries.
        :raise LookupError: When an id is not valid.
        """
        return await asyncio.gather(*(self.get(id) for id in ids))

    async def add_message_meta_infos(self, id: str, meta_info_name: str, info: Any):
        """Add a store-related meta info to a message.

        :param id: Store-dependant id.
        :param meta_info_name: Name of the meta info to create/update.
        :param info: Info value.
        :raise LookupError: When the id does not name a stored message.
        """
        meta = await self._get_storemeta(id)
        meta[meta_info_name] = info
        await self._set_storemeta(id, meta)

    async def get_message_meta_infos(self, id: str, meta_info_name: str | None = None) -> Any:
        """Get a store-related meta info for a message.

        :param id: Store-dependant id.
        :param meta_info_name: (optional) Name of the meta info to
            retrieve. If `None`, the whole dict is returned.
        :raise LookupError: When the id does not name a stored message.
        """
        meta = (await self._get_storemeta(id))["meta"]
        return meta if meta_info_name is None else meta[meta_info_name]

    async def get_message_state(self, id: str) -> Message.State_:
        """Get a message's state. Shorthand for
        :meth:`get_message_meta_infos` with `"state"`

        :param id: Store-dependant id.
        :raise LookupError: When the id does not name a stored message.
        """
        return await self.get_message_meta_infos(id, "state")

    async def change_message_state(self, id: str, new_state: Message.State_):
        """Change a message's state. Shorthand for
        :meth:`add_message_meta_infos` with `"state"`

        :param id: Store-dependant id.
        :param new_state: ye.
        :raise LookupError: When the id does not name a stored message.
        """
        await self.add_message_meta_infos(id, "state", new_state)

    async def add_sub_message_state(self, id, sub_id, state):
        -NotImplemented

    async def set_state_to_worst_sub_state(self, id):
        -NotImplemented

    async def get_preview_str(self, id: str) -> Message:
        """A deprecated weirdness which absolutely does not return str.

        Retrieve a message by id, then craft a fake message with
        for payload the first 1000 characters of a textual
        representation of the actual message's payload.

        TODO: remove it, or rework it; it should return a str.

        :param id: Store-dependant id.
        :raise LookupError: When the id does not name a stored message.
        :return: go figure.
        """
        msg = await self.get_msg_content(id)
        try:
            msg.payload = str(msg.payload)[:1000]
        except Exception:
            msg.payload = repr(msg.payload)[:1000]
        return msg

    async def get_msg_content(self, id: str) -> Message:
        """Deprecated shorthand for :meth:`get` + `result['message']`.

        TODO: remove it.

        :param id: Store-dependant id.
        :raise LookupError: When the id does not name a stored message.
        :return: go figure.
        """
        return (await self.get(id))["message"]

    async def get_payload_str(self, id: str) -> str:
        """Shorthand to get a representation of the message's payload.

        Essentially equivalent to using :func:`str` with the
        `['message'].payload` obtained with :meth:`get`.

        :param id: Store-dependant id.
        :raise LookupError: When the id does not name a stored message.
        :return: Textual representation of the payload.
        """
        return str(await self._get_message(id))

    async def is_regex_in_msg(self, id: str, rtext: str | re.Pattern[str]):
        """Shorthand pattern check `rtext` in :meth:`get_payload_str`.

        :param id: Store-dependant id.
        :param rtext: String or regular expression.
        :return: True if it matches False otherwise.
        """
        return re.match(rtext, await self.get_payload_str(id)) is not None

    async def is_txt_in_msg(self, id: str, text: str):
        """Shorthand to check if `text` is in :meth:`get_payload_str`.

        :param id: Store-dependant id.
        :param text: String or regular expression.
        :return: True if it matches False otherwise.
        """
        return text in await self.get_payload_str(id)

    @staticmethod
    def _search_meta_filter_sort(meta_search: dict, results: list):
        0

    async def search(self, _: ...):
        -NotImplemented("search")


# implementations {{{1
# null/noop {{{2
class NullMessageStoreFactory(MessageStoreFactory):
    """A no-op message store.

    Always successful, nothing is stored. I don't know what that
    implies, it's scary, but it's needed.

    That is: storage/update/delete operations are always successful,
    however retrieve operations do not return anything relevant.
    Mostly, retrieved messages will be :obj:`NotImplemented`.
    """

    def _new_store(self, store_id: str) -> MessageStore:
        return NullMessageStore()


class NullMessageStore(MessageStore):
    # made it a singleton for now, we could instead choose to have
    # a 'logger: logger | None' constructor parameter
    _instance: NullMessageStore | None = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def _store(self, msg: Message) -> str:
        return ""

    async def _delete(self, id: str):
        pass

    async def _total(self) -> int:
        return 0

    async def _get_message(self, id: str) -> Message:
        return NotImplemented

    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        return {}

    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        pass

    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        return []


# memory {{{2
class MemoryMessageStoreFactory(MessageStoreFactory):
    """An in-memory message store.

    All messages are lost at pypeman stop.
    """

    def _new_store(self, store_id: str) -> MessageStore:
        return MemoryMessageStore()


class MemoryMessageStore(MessageStore):
    class MemoryStoredEntry_(TypedDict):
        meta: dict[str, Any]
        message: dict[str, Any]  # for now messages are still `to_dict`'d
        timestamp: datetime

    def __init__(self):
        # this is a list so that it can be sort-inserted by timestamp
        self._sorted: list[MemoryMessageStore.MemoryStoredEntry_] = []
        # accompanying mapping for direct accesses by id
        self._mapped: dict[str, MemoryMessageStore.MemoryStoredEntry_] = {}
        # 2 references are stored to the same entry for 2 different usage

    async def _store(self, msg: Message) -> str:
        if msg.uuid in self._mapped:
            return msg.uuid

        entry: MemoryMessageStore.MemoryStoredEntry_ = {
            "meta": {},
            "message": msg.to_dict(),
            "timestamp": msg.timestamp,
        }

        # sort-insert with assumption that the new message is more likely to go at the end
        for k in range(len(self._sorted), 0, -1):
            if self._sorted[k - 1]["timestamp"] <= msg.timestamp:
                self._sorted.insert(k, entry)
                break
        else:
            self._sorted.insert(0, entry)

        self._mapped[msg.uuid] = entry
        return msg.uuid

    async def _delete(self, id: str):
        msg = self._mapped.pop(id)
        self._sorted.remove(msg)

    async def _total(self) -> int:
        return len(self._sorted)

    async def _get_message(self, id: str) -> Message:
        return Message.from_dict(self._mapped[id]["message"])

    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        return deepcopy(self._mapped[id]["meta"])

    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        self._mapped[id]["meta"] = meta

    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        # test degenerate cases first
        if (
            not self._sorted
            or (start_dt and self._sorted[-1]["timestamp"] < start_dt)
            or (end_dt and end_dt <= self._sorted[0]["timestamp"])
        ):
            return []

        start, end = 0, len(self._sorted)

        if start_dt is not None:
            for start, entry in enumerate(self._sorted):
                if start_dt <= entry["timestamp"]:
                    break  # start found
            # degenerate cases test ensure it's found

        if end_dt is not None:
            for end, entry in enumerate(self._sorted[start + 1 :]):  # noqa: E203 (black formatting)
                if end_dt <= entry["timestamp"]:
                    break  # end found
            # degenerate cases test ensure it's found

        return [msg["message"]["uuid"] for msg in self._sorted[start:end]]


# file {{{2
class FileMessageStoreFactory(MessageStoreFactory):
    """A filesystem-based message store.

    Messages are stored in file pairs under
    `<base_path>/<store_id>/<year>/<month>/<day>/`:
        * `<timestamp>_<uuid>.meta`  the store-related meta;
        * `<timestamp>_<uuid>`       the actual message.

    The `<timestamp>` part is as :obj:`FileMessageStore.DATE_FORMAT`.
    """

    # TODO: see if both the parameter and the property can be harmonized
    def __init__(self, path: str | PurePath):
        super().__init__()
        # sanity check
        if not isinstance(path, (str, PurePath)):
            raise PypemanConfigError("file message store requires a path")
        self.base_path = PurePath(path)

    def _new_store(self, store_id: str) -> MessageStore:
        return FileMessageStore(self.base_path, store_id)


class FileMessageStore(MessageStore):
    DATE_FORMAT = "%Y%m%d_%H%M%S%f"  # 21 guaranteed length (8+1+12)
    _MSG_RE = re.compile(
        """
        ^
            (?P<year>[0-9]{4})
            (?P<month>[0-9]{2})
            (?P<day>[0-9]{2})
        _
            (?P<hour>[0-9]{2})
            (?P<minute>[0-9]{2})
            ( # an older format didn't have sec/usec
                (?P<second>[0-9]{2})
                (?P<microsecond>[0-9]{6})
            )?
        _
            (?P<uid>[0-9a-zA-Z]*)
        $
        """,
        re.VERBOSE,
    )

    # TODO: reporting an older todo about making more things async:
    #   see https://github.com/python/asyncio/wiki/ThirdParty#filesystem
    #   as well as the refered https://github.com/Tinche/aiofiles/
    #   as of now, even tho everything is marked async, none is-

    def __init__(self, path: PurePath, store_id: str):
        super().__init__()
        # TODO: see if property name and parameter can be changed/harmonized
        self.base_path = Path(path, store_id)
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def _store(self, msg: Message) -> str:
        return ""

    async def _delete(self, id: str):
        pass

    async def _total(self) -> int:
        return sum(len(files) for _, _, files in self.base_path.walk())

    async def _get_message(self, id: str) -> Message:
        return NotImplemented

    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        return {}

    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        pass

    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        return []


# TODO: this is specific to the FileMessageStore, remove the top-level alias
DATE_FORMAT = FileMessageStore.DATE_FORMAT


# (modeline) {{{1
# vim: se tw=101:
