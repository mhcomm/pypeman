"""Message store interface specification and shipped implementations.

Every instantiated channel (:class:`pypeman.BaseChannel`) may be
associated a message store :class:`MessageStore`.

A message store provides the ability to store and restore
a :class:`pypeman.Message` as well as a store-related meta (distinct
from the message's meta). Once a message is stored, it can be referred
to through a specific id (also distinct from the message's UUID).

Stores should not be created directly; the factory pattern should be
used instead. In a pypeman project is usually a unique message store
factory. The following ones are provided, differing only by there
backing storage:
    * :class:`NullMessageStoreFactory`,
    * :class:`MemoryMessageStoreFactory`,
    * :class:`FileMessageStoreFactory`.

At channel level, a store is associated at creation by handing it the
factory. Every new message passing through the channel's
:meth:`BaseChannel.handle` method will be stored (ie. some time before
calling any node's :meth:`BaseNode.process`).

The message store is also involved in the :attr:`BaseNode.store_meta`
mechanism (store declared meta of passing-by messages after processing).

A channel associated with a message store will enable replaying
a message through :meth:`BaseChannel.replay`. The
:class:`pypeman.plugins.remoteadmin.RemoteAdminPlugin` plugin provides
a more hands-on access to it on a running pypeman project.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from abc import ABC
from abc import abstractmethod
from copy import deepcopy
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from pathlib import PurePath
from sqlite3 import IntegrityError
from sqlite3 import OperationalError
from typing import Any
from typing import Callable
from typing import Literal
from typing import TypedDict

import aiosqlite
from dateutil import parser as dateutilparser

from .errors import PypemanConfigError
from .message import Message


logger = logging.getLogger(__name__)


class MessageStoreFactory(ABC):
    """A :class:`MessageStoreFactory` instance can generate related
    :class:`MessageStore` instances for a specific `store_id`. The
    later in turn is needed to store :class:`Message`s.

    Note for implementing classes: the constructor is expected to do
    precisely nothing, that is no new dir/file on fs or table in db...

    :abstract:
    """

    def __init__(self):
        self._stores: dict[str, MessageStore] = {}

    @abstractmethod
    def _new_store(self, store_id: str) -> MessageStore:
        """(implementation) Create a new store refered to as `store_id`.

        Note for implementing classes: this function doesn't perform
        any heavy work (not async), instead the returned store's
        :meth:`MessageStore.start` does.

        :param store_id: Identifier of the corresponding message store.
        :return: New :class:`MessageStore` instance.
        """

    @abstractmethod
    async def _delete_store(self, store: MessageStore):
        """(implementation) Wipe out the store.

        Note to implementing classes: the store might not have been
        started :meth:`MessageStore.start`, and there is no logic
        calling :meth:`delete` on all the stored messages.

        :param store: Store instanciated by this very class.
        """

    def get_store(self, store_id: str) -> MessageStore:
        """Return a store corresponding to the given `store_id`.

        :param store_id: Identifier of the corresponding message store.
        :return: New or existing :class:`MessageStore` instance.
        """
        existing = self._stores.get(store_id)
        return existing or self._stores.setdefault(store_id, self._new_store(store_id))

    def list_store(self) -> list[str]:
        """List of instanciated stores' `store_id`.

        Remark: this only lists stores instanciated _during this run_
        of the pypeman project; if other stores where once created but
        are not currently used, they will not appear.

        :return: List.
        """
        return list(self._stores.keys())

    async def delete_store(self, store_id: str):
        """Delete the whole store and everything associated with it.

        Really only meant to be used in testing.
        !CAUTION! this cannot be undone.

        This is a method on the factory and not on the store for book
        keeping reasons.

        :raise KeyError: When the `store_id` does not exist.
        """
        existing = self._stores.pop(store_id)
        logger.debug(f"deleting store {store_id} ({existing!r}))")
        await self._delete_store(existing)
        # private usage in friend class
        existing._cached_total = 0


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
        * :meth:`_stop` -- default is no-op
        * :meth:`get_many` -- default might be inefficient

    :abstract:
    """

    class StoredEntry_(TypedDict):
        """Data format used when retrieving messages."""

        id: str  # store-dependant id, different from message.uuid
        meta: dict[str, Any]  # store-related meta, different from message.meta
        # TODO: ideally this `Any` would be replaced with `list[str]` with explicit checks at access points
        message: Message
        state: Message.State_  # TODO: remove in favor of _['meta']['state']

    async def _start(self):
        """(implementation) Called at startup to initialize the store.

        This method is not marked as abstract and thus not required.
        """

    async def _stop(self):
        """(implementation) Called at teardown to finalize the store.

        This method is not marked as abstract and thus not required.
        """

    @abstractmethod
    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        """(implementation) Store a message.

        Implementation must ensure that all operations with an id
        obtained this way are valid until a matching :meth:`_delete`.

        An exact same message (same uuid) should not be stored twice;
        the implementation may raise an error if it detects such.

        :param msg: Message to be stored.
        :param ini_meta: Store-related initial meta.
        :return: Store-dependant id.
        :raise ValueError: When a double-store (same uuid) is detected.
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

        These must be sorted by (increasing) timestamps.

        Note for implementing classes: `start_dt < end_dt` is
        verified, as well as `self.total() != 0`. However either or
        both datetimes given may be completely out of the range of
        stored entries.

        :param start_dt: (optional) Inclusive lower bound, or None.
        :param end_dt: (optional) Exclusive upper bound, or None.
        """

    async def start(self):
        """Called at startup to initialize the store."""
        await self._start()
        self._cached_total = await self._total()
        logger.debug(f"store started successfully {self!r}: {self._cached_total} message(s)")

    async def stop(self):
        """Called at teardown to finalize the store."""
        # TODO: !!! this is not hooked in yet! I need to add it to channel stop
        await self._stop()

    async def store(self, msg: Message) -> str:
        """Store a message in the store.

        Its state is right away initialized to :obj:`Message.PENDING`.

        An exact same message (same uuid) should not be stored twice.

        :param msg: Message to store.
        :return: Id for this specific message.
        :raise ValueError: When a double-store (same uuid) is detected.
        """
        id = await self._store(msg, {"state": Message.PENDING})
        self._cached_total += 1
        return id

    async def delete(self, id: str):
        """Delete a message in the store corresponding to the given id.

        Useful for tests and (maybe in the future) for cleanup.
        !CAUTION! this cannot be undone.

        :param id: Store-dependant id.
        :return: Same as :meth:`get`. This will be removed.
        :raise LookupError: When the id does not name a stored message.
        """
        # TODO: don't, unnecessary
        entry = await self.get(id)
        logger.debug(f"deleting message {id} {self!r}")
        await self._delete(id)
        self._cached_total -= 1
        return entry

    # TODO: this is not async anymore, marking it non-async shows it is a cheap operation
    async def total(self) -> int:
        """Number of messages stored at this time.

        Marked async but will no longer be at some point.

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

        This method does not expect being used to retrieve a single
        entry; see :meth:`get` instead.

        Some implementation may override this method to be more
        efficient than repeatedly calling :meth:`get` in a loop.
        (Which is more or less what this default implementation does.)

        Note for implementing classes: `1 < len(ids)` is verified, as
        well as `self.total() != 0`.

        :param ids: Sequence of store-dependant id.
        :return: The sequence of store entries.
        :raise LookupError: When an id does not name a stored message.
        """
        return await asyncio.gather(*map(self.get, ids))

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

    async def get_message_meta_infos(self, id: str, meta_info_name: str | None = None) -> Any | None:
        """Get a store-related meta info for a message.

        :param id: Store-dependant id.
        :param meta_info_name: (optional) Name of the meta info to
            retrieve. If `None`, the whole dict is returned.
        :raise LookupError: When the id does not name a stored message.
        """
        meta = await self._get_storemeta(id)
        return meta if meta_info_name is None else meta.get(meta_info_name)

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

    async def add_sub_message_state(self, id: str, sub_id: str, state: str):
        """
        Add a state to the meta 'submessages_state_history" of a message
        submessages_state_history meta is a list of dicts of this form:
        {
            "sub_id": <sub_id>,
            "state": <state>,
            "timestamp": <time.time()>
        }

        :param id: Message specific store id.
        :param sub_id: The sub message's id (could be the same as the id).
        :param state: Target state.
        """
        try:
            submessages_state_history = await self.get_message_meta_infos(id, "submessages_state_history")
            if submessages_state_history is None:
                submessages_state_history = []
        except KeyError:
            submessages_state_history = []
        submessages_state_history.append(
            {
                "sub_id": sub_id,
                "state": state,
                "timestamp": time.time(),
            }
        )
        await self.add_message_meta_infos(id, "submessages_state_history", submessages_state_history)

    async def set_state_to_worst_sub_state(self, id: str):
        """
        Change the `id` message state to the worst state stored in submessages_state_history

        :param id: Message specific store id.
        """
        try:
            submessages_state_history = await self.get_message_meta_infos(id, "submessages_state_history")
        except LookupError:
            submessages_state_history = []
        if not submessages_state_history:
            raise IndexError("No sub message state stored, cannot choose the worst")
        worst_state = submessages_state_history[0]["state"]
        for submsg_info in submessages_state_history:
            state = submsg_info["state"]
            if Message.STATES_PRIORITY.index(worst_state) < Message.STATES_PRIORITY.index(state):
                worst_state = state
        await self.change_message_state(id, worst_state)

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
        # rational for the pragma: this code was ported over 1:1 from ol' msgstore;
        # in python no object throws on `str()` unless done intentionally in `__str__`,
        # which nobody does or should even think about doing (keep in mind that
        # `__repr__` could too.. so does literally anything actually)
        except Exception:  # pragma: no cover
            msg.payload = repr(msg.payload)[:1000]
        return msg

    async def get_msg_content(self, id: str) -> Message:
        """Deprecated shorthand for :meth:`get` + `result['message']`.

        TODO: remove it.

        :param id: Store-dependant id.
        :raise LookupError: When the id does not name a stored message.
        :return: go figure.
        """
        return await self._get_message(id)

    async def get_payload_str(self, id: str) -> str:
        """Shorthand to get a representation of the message's payload.

        Essentially equivalent to using :func:`str` with the
        `['message'].payload` obtained with :meth:`get`.

        :param id: Store-dependant id.
        :raise LookupError: When the id does not name a stored message.
        :return: Textual representation of the payload.
        """
        return str((await self._get_message(id)).payload)

    async def is_regex_in_msg(self, id: str, rtext: str | re.Pattern[str]):
        """Shorthand pattern check `rtext` in :meth:`get_payload_str`.

        :param id: Store-dependant id.
        :param rtext: String or regular expression.
        :return: True if it matches False otherwise.
        """
        return re.search(rtext, await self.get_payload_str(id)) is not None

    async def is_txt_in_msg(self, id: str, text: str):
        """Shorthand to check if `text` is in :meth:`get_payload_str`.

        :param id: Store-dependant id.
        :param text: String or regular expression.
        :return: True if it matches False otherwise.
        """
        return text in await self.get_payload_str(id)

    async def search(
        self,
        count: int = 10,
        start_id: str | None = None,
        order_by: Literal["timestamp", "state", "-timestamp", "-state"] | str = "timestamp",
        group_by: Literal["state"] | str | None = None,
        start_dt: datetime | str | None = None,
        end_dt: datetime | str | None = None,
        text: str | None = None,
        rtext: str | None = None,
        meta: dict[str, str] | None = None,
    ) -> list[StoredEntry_] | dict[str, list[StoredEntry_]]:
        """Fetch a list of message entries according to specification.

        :param count: Number of entries to return. This is a maximum:
            result might consist of fewer matches. `count` only applies
            time-wise, that is it will result in the `count` first (or
            last if `order_by` is reversed) messages (within the time
            span `start_dt`..`end_dt`).
        :param start_id: (optional): If set, start search from this id
            (excluded from result).

        :param order_by: Sort results by a key. If the key starts with
            '-' the ordering is reversed. Default: 'timestamp', allowed
            values: ['timestamp', 'state', 'meta_<name>'].
        :param group_by: Group the results by a key. Instead of a list,
            the result will be a dict that maps from the various values
            found for the key to a list of matching entries. Value like
            'meta_<name>' may be used to group by meta.

        :param start_dt: (optional) Limit to messages since this time.
        :param end_dt: (optional) Limit to messages until this time.
        :param text: (optional) Text to search in message content.
        :param rtext: (optional) Pattern to search in message content.
        :param meta: (optional): Meta search options (see full doc).

            Message meta are always stored as a list of strings when
            going through `BaseNode.store_meta`. (If an in-store entry
            is not a list, it will be handled as if a list of a single
            item.) If any value from the inspected list of meta
            correspond to the query, the message is selected.

            In the key/value pairs of the `meta` argument to `search`,
            keys are processed as follow:

            * `exact_<name>`: only match with exact value;

            * `text_<name>`: string to search in meta value;
            * `rtext_<name>`: string regex to search in meta value;

            * `start_<name>` / `end_<name>`: filter a range, values are
                interpreted and compared as numbers when possible;

            Reminder: when called through web API, ie from `list_msgs`
            in pypeman/plugins/remoteadmin/views.py, these keys, in the
            request params, are prefixed with "meta_" (eg.
            "meta_state" "meta_rtext_url") but here they no longer are.

        :return: List of fitting message entries or, if `group_by` is
            given, dict of group to list of fitting message entries.
        """
        logger.debug(f"searching {self!r} for {count} messages, start at {start_id}")

        # early dum check (as per contract, don't call _span_select in this case)
        if 0 == await self.total():
            return [] if group_by is None else {}

        # TODO: phase out the 'str' option, this should be caller responsibility
        if isinstance(start_dt, str):
            start_dt = dateutilparser.isoparse(start_dt)
        if isinstance(end_dt, str):
            end_dt = dateutilparser.isoparse(end_dt)

        if start_dt and end_dt:
            assert start_dt <= end_dt, "backward time span given"
            assert start_dt != end_dt, "empty time span given"

        # only one of the two is non-none at once
        order_by_payload = None
        order_by_meta = None
        reverse = "-" == order_by[0]
        if reverse:
            order_by = order_by[1:]
        if order_by.startswith("meta_"):
            order_by_meta = order_by[5:]
        else:
            assert order_by in {"timestamp", "state"}
            order_by_payload: ... = order_by

        # only one of the two may be non-none at once
        group_by_payload = None
        group_by_meta = None
        if group_by:
            if group_by.startswith("meta_"):
                group_by_meta = group_by[5:]
            else:
                assert group_by in {"state"}
                group_by_payload: ... = group_by

        # note that it compiles any rtext and already raises if invalid
        payload_filt = _MsgFilt(order_by_payload, group_by_payload, text, rtext)
        meta_filt = _MetaFilt(order_by_meta, group_by_meta, meta)

        span = await self._span_select(start_dt, end_dt)
        if reverse:
            span.reverse()
        if start_id:
            # +1: excluding start id; note that this already raises if `start_id` is not in
            cut = span.index(start_id) + 1
            span = span[cut:]

        filtered: list[MessageStore.StoredEntry_] = []

        needed = count
        span_len = len(span)
        scan_head = 0
        # continuously grab however-many messages needed, filter and append
        # until fulfilled (0 == needed) or exhausted (scan_head >= span_len)
        while needed and scan_head < span_len:
            available = span_len - scan_head
            grab = min(needed, available)

            scan_ahead = scan_head + grab
            chunk = (
                [await self.get(span[0])]
                if 1 == grab  # as per `get_many`'s contract
                else await self.get_many(span[scan_head:scan_ahead])
            )
            filtered.extend(it for it in chunk if payload_filt.filter(it) and meta_filt.filter(it))

            needed = count - len(filtered)
            scan_head = scan_ahead

        # a word to (whoever) when :param:`order_by` is by timestamp,
        # ie `order_by_payload == "timestamp"`, `filtered` will always
        # be already sorted...
        order = meta_filt.order if order_by_meta else payload_filt.order
        filtered.sort(key=order, reverse=reverse)
        if not group_by:
            logger.debug(f"found {len(filtered)} message(s)")
            return filtered

        group = meta_filt.group if group_by_meta else payload_filt.group
        grouped: dict[str, list[MessageStore.StoredEntry_]] = {}
        for it in filtered:
            # at this point filtered is also sorted, so each individual group will be too
            grouped.setdefault(group(it), []).append(it)
        logger.debug(f"found {len(filtered)} message(s), in {len(grouped)} group(s)")
        return grouped


class _MsgFilt:
    """(internal) see :meth:`MessageStore.sort`

    used to filter/order/group entries by there message's payload
    """

    def __init__(
        self,
        order_by: Literal["timestamp", "state"] | None,
        group_by: Literal["state"] | None,
        text: str | None,
        rtext: str | None,
    ):
        # `self.order()` (resp. `self.group()`) are never called
        # if `order_by` (resp. `group_by`) is None
        self.order_by = order_by
        self.group_by = group_by
        self.text = text
        self.regex = re.compile(rtext) if rtext else None
        # no filter, will always return True
        self.nofilt = not self.text and not self.regex

    def filter(self, entry: MessageStore.StoredEntry_) -> bool:
        ":return: true if the entry is to be kept in the result"
        if self.nofilt:
            return True
        astr = str(entry["message"].payload)
        if self.text and self.text not in astr:
            return False
        if self.regex and self.regex.search(astr) is None:
            return False
        return True

    def order(self, entry: MessageStore.StoredEntry_) -> Any:
        """:return: SupportsRichComparison (see usage)"""
        if "timestamp" == self.order_by:
            return entry["message"].timestamp
        if "state" == self.order_by:
            return entry["meta"]["state"]
        assert None, "unreachable"  # pragma: no cover

    def group(self, entry: MessageStore.StoredEntry_) -> str:
        ":return: group name for entry"
        if "state" == self.group_by:
            return entry["meta"]["state"]
        assert None, "unreachable"  # pragma: no cover


class _MetaFilt:
    """(internal) see :meth:`MessageStore.sort`

    used to filter/order/group entries by there store-related meta

    remark: in constructor params, `order_by` and `group_by` are
    without the leading 'meta_'
    """

    @staticmethod
    def _isfloat(s: str):
        """used with start_/end_"""
        try:
            float(s)
            return True
        except ValueError:
            return False

    class _FILTERS:
        @staticmethod
        def exact(value: str) -> Callable[[str], bool]:
            return lambda info: info == value

        @staticmethod
        def text(text: str) -> Callable[[str], bool]:
            return lambda info: text in info

        @staticmethod
        def rtext(rtext: str) -> Callable[[str], bool]:
            regex = re.compile(rtext)
            return lambda info: bool(regex.search(info))

        @staticmethod
        def start(start: str) -> Callable[[str], bool]:
            fstart = float(start)
            return lambda info: _MetaFilt._isfloat(info) and float(info) >= fstart

        @staticmethod
        def end(end: str) -> Callable[[str], bool]:
            fend = float(end)
            return lambda info: _MetaFilt._isfloat(info) and float(info) <= fend

    def __init__(self, order_by: str | None, group_by: str | None, meta: dict[str, str] | None):
        # `self.order()` (resp. `self.group()`) are never called
        # if `order_by` (resp. `group_by`) is None
        # `str()` is for typing reason so that it's a non-none str when used
        self.order_by = str(order_by)
        self.group_by = str(group_by)
        # each key is a `meta_info_name`, and each value is a list of filter functions
        self.filters: dict[str, list[Callable[[str], bool]]] = {}
        if meta:
            for key, value in meta.items():
                filt_name, _, meta_name = key.partition("_")
                filt = getattr(_MetaFilt._FILTERS, filt_name)
                self.filters.setdefault(meta_name, []).append(filt(value))

    def filter(self, entry: MessageStore.StoredEntry_) -> bool:
        """:return: true if the entry is to be kept in the result

        every filter on meta must match, but on any info of the meta
        """
        meta = entry["meta"]
        # note that no filter will return True as expected
        for name, filts in self.filters.items():
            m = meta.get(name, [])
            info: list[Any] = m if isinstance(m, list) else [m]
            for filt in filts:  # .                        for each filter for this meta info,
                if not any(filt(str(i)) for i in info):  # if none of the values matche
                    return False  # .                      then stop here
            # the lines above are roughly equivalent to:
            # ( filts[0](info[0]) OR filts[0](info[1]) OR .. ) AND ( filts[1](info[0]) OR .. ) AND ..
        return True

    def order(self, entry: MessageStore.StoredEntry_) -> Any:
        """:return: :class:`str` of the meta

        uses the first value if the meta is a list
        for an empty list will use an empty string
        same if this meta is not present
        """
        m = entry["meta"].get(self.order_by, [])
        info: list[Any] = m if isinstance(m, list) else [m]
        return str(info[0]) if info else ""

    def group(self, entry: MessageStore.StoredEntry_) -> str:
        """:return: group name for entry

        uses the first value if the meta is a list
        for an empty list will use an empty string
        same if this meta is not present

        TODO(potential evolution): if deemed interesting,
            messages could be present in multiple groups
        """
        m = entry["meta"].get(self.group_by, [])
        info: list[Any] = m if isinstance(m, list) else [m]
        return str(info[0]) if info else ""


class NullMessageStoreFactory(MessageStoreFactory):  # pragma: no cover
    """A no-op message store.

    Always successful, nothing is stored. I don't know what that
    implies, it's scary, but it's needed.

    That is: storage/update/delete operations are always successful,
    however retrieve operations do not return anything relevant.
    Mostly, retrieved messages will be :obj:`NotImplemented`.
    """

    def _new_store(self, store_id: str) -> MessageStore:
        return NullMessageStore()

    async def _delete_store(self, store: MessageStore):
        pass


class NullMessageStore(MessageStore):  # pragma: no cover
    # made it a singleton for now, we could instead choose to have
    # a 'logger: logger | None' constructor parameter
    _instance: NullMessageStore | None = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        """broken invariant: does not return :class:`str` because
        (at least at time of writing) a None check is used in
        :meth:`BaseChannel.handle` and it fails at least 1 test..."""
        return None

    async def _delete(self, id: str):
        pass

    async def _total(self) -> int:
        return 0

    async def _get_message(self, id: str) -> Message:
        """broken invariant: does not return :class:`Message`"""
        return NotImplemented

    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        return {}

    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        pass

    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        return []


class MemoryMessageStoreFactory(MessageStoreFactory):
    """An in-memory message store.

    All messages are lost at pypeman stop.
    """

    def _new_store(self, store_id: str) -> MessageStore:
        return MemoryMessageStore()

    async def _delete_store(self, store: MessageStore):
        assert isinstance(store, MemoryMessageStore)  # type narrowing
        # private usage in friend class
        store._sorted.clear()
        store._mapped.clear()


class MemoryMessageStore(MessageStore):
    class MemoryStoredEntry_(TypedDict):
        meta: dict[str, Any]
        message: dict[str, Any]  # for now messages are still `to_dict`-ified, this may change
        timestamp: datetime

    def __init__(self):
        super().__init__()
        # this is a list so that it can be sort-inserted by timestamp
        self._sorted: list[MemoryMessageStore.MemoryStoredEntry_] = []
        # accompanying mapping for direct accesses by id
        self._mapped: dict[str, MemoryMessageStore.MemoryStoredEntry_] = {}
        # 2 references are stored to the same entry for 2 different usage

    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        if msg.uuid in self._mapped:
            raise ValueError(f"message {msg} is being stored again in {self}")

        entry: MemoryMessageStore.MemoryStoredEntry_ = {
            "meta": ini_meta,
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
        if start_dt and self._sorted[-1]["timestamp"] < start_dt:
            return []
        if end_dt and end_dt <= self._sorted[0]["timestamp"]:
            return []

        start, end = 0, len(self._sorted)

        if start_dt is not None:
            for start, entry in enumerate(self._sorted):
                if start_dt <= entry["timestamp"]:
                    break  # start found
            # no `else`: degenerate cases test ensure it's found

        if end_dt is not None:
            for end in range(start + 1, len(self._sorted)):
                if end_dt <= self._sorted[end]["timestamp"]:
                    break  # end found
            else:
                end = len(self._sorted)

        return [msg["message"]["uuid"] for msg in self._sorted[start:end]]


class FileMessageStoreFactory(MessageStoreFactory):
    """A filesystem-based message store.

    Messages are stored in file pairs under
    `<base_path>/<store_id>/<year>/<month>/<day>/`:
        * `<timestamp>_<uuid>.meta`  the store-related meta;
        * `<timestamp>_<uuid>`       the actual message.

    The `<timestamp>` part is as :obj:`FileMessageStore.DATE_FORMAT`.
    """

    # TODO: see if both the parameter and the property names can be harmonized/privatized
    def __init__(self, path: str | PurePath):
        super().__init__()
        # sanity check
        if not isinstance(path, (str, PurePath)):
            raise PypemanConfigError("file message store requires a path")  # pragma: no cover
        self.base_path = PurePath(path)

    def _new_store(self, store_id: str) -> MessageStore:
        # there **must not** be any of these char in `store_id`
        # (matches the set of illegal DOS/Windows path characters)
        assert not set(r'<>:"/\|?*') & set(store_id), f"store id {store_id!r} contains annoying characters"
        return FileMessageStore(self.base_path, store_id)

    async def _delete_store(self, store: MessageStore):
        assert isinstance(store, FileMessageStore)  # type narrowing
        # private usage in friend class
        store._latest = datetime.min
        store._earliest = datetime.max

        def rmrf(path: Path):
            "helper for older pathlib without :meth:`Path.walk`"
            for child in path.iterdir():
                if not child.is_symlink() and child.is_dir():
                    rmrf(child)
                else:
                    child.unlink()
            path.rmdir()

        if store.base_path.exists():
            rmrf(store.base_path)
        else:
            logger.warning(f"deleting file message store that was not started: {self.base_path!r}")


class FileMessageStore(MessageStore):
    """

    Ids are of the form `'{date}_{time}_{uuid}'`. Again, message store
    ids are **implementation details** and **must not be inspected**
    other than for debugging/logging/.. purposes!
    """

    DATE_FORMAT = "%Y%m%d_%H%M%S%f"  # 21 guaranteed length (8+1+12)
    _PARSE_ID = re.compile(
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
    #   (lastely this likely doesnt matter, I'll try to benchmark both
    #   somewhat to prove otherwise.. I do belive it would speed up
    #   `get_many` quite a bit but not so sure)

    def __init__(self, path: PurePath, store_id: str):
        super().__init__()
        # TODO: see if property name and parameter names can be changed/harmonized/privatized
        self.base_path = Path(path, store_id)
        # reminder: don't touch file system here! wait for `start`

    async def _start(self):
        self.base_path.mkdir(parents=True, exist_ok=True)

        every = sorted(
            file.name
            for file in self.base_path.glob("*/*/*/*")  # <y>/<m>/<d>/<file>
            if file.is_file() and FileMessageStore._PARSE_ID.match(file.name) is not None
        )
        if not every:
            self._earliest = datetime.max
            self._latest = datetime.min

        else:
            res = FileMessageStore._PARSE_ID.match(every[0])
            ymd_hmsf = res.group("year", "month", "day", "hour", "minute", "second", "microsecond")
            self._earliest = datetime(*(int(t or 0) for t in ymd_hmsf))

            res = FileMessageStore._PARSE_ID.match(every[-1])
            ymd_hmsf = res.group("year", "month", "day", "hour", "minute", "second", "microsecond")
            self._latest = datetime(*(int(t or 0) for t in ymd_hmsf))

    def _dt2dir(self, dt: datetime | date) -> Path:
        """Build the dir path where messages for day `dt` are/would be.

        For use with an already assembled id prefer :meth:`_id2file`.
        This method is implementation detail.

        :param dt: Date.
        :return: Directory path.
        """
        return self.base_path / f"{dt.year:04}" / f"{dt.month:02}" / f"{dt.day:02}"

    def _id2file(self, id: str) -> Path:
        """For a vaild `id`, build the corresponding file path.

        For use with a plain dt/timestamp prefer :meth:`_dt2dir`.
        This method is implementation detail.

        :param id: `'{date}_{time}_{uuid}'`.
        :return: Suffix-less file path.
        :raise LookupError: Cannot parse `id`.
        """
        res = FileMessageStore._PARSE_ID.match(id)
        if not res:
            raise LookupError(f"unknown id format {id!r}")
        gd = res.groupdict()
        return self.base_path / gd["year"] / gd["month"] / gd["day"] / id

    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        id = f"{msg.timestamp.strftime(FileMessageStore.DATE_FORMAT)}_{msg.uuid}"
        # at time of writing :class:`Message` isn't correctly typed (*at all)
        # these two lines are to enforce correct type (the aliase can be removed when updated)
        timestamp: ... = msg.timestamp
        timestamp: datetime
        msg_path = self._dt2dir(timestamp) / id

        if msg_path.exists():
            raise ValueError(f"message {msg} is being stored again in {self}")

        msg_path.parent.mkdir(parents=True, exist_ok=True)
        with msg_path.open("w") as f:
            f.write(msg.to_json())
        with msg_path.with_suffix(".meta").open("w") as f:
            json.dump(ini_meta, f)

        if timestamp < self._earliest:
            self._earliest = timestamp
        if self._latest < timestamp:
            self._latest = timestamp

        return id

    async def _delete(self, id: str):
        msg_path = self._id2file(id)
        msg_path.unlink(missing_ok=True)
        msg_path.with_suffix(".meta").unlink(missing_ok=True)

    async def _total(self) -> int:
        if not self.base_path.exists():
            return 0
        return sum(
            file.is_file() and FileMessageStore._PARSE_ID.match(file.name) is not None
            for file in self.base_path.glob("*/*/*/*")  # <y>/<m>/<d>/<file>
        )

    async def _get_message(self, id: str) -> Message:
        msg_path = self._id2file(id)
        if not msg_path.exists():
            raise LookupError(f"no message stored under {id!r}")
        with msg_path.open("r") as f:
            return Message.from_json(f.read())

    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        meta_path = self._id2file(id).with_suffix(".meta")
        if not meta_path.exists():
            raise LookupError(f"no meta stored under {id!r}")
        with meta_path.open("r") as f:
            content = f.read().strip()
        # (uuuuuuugh) still handle oldass format
        return json.loads(content) if "{" == content[:1] else {"state": content}

    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        with self._id2file(id).with_suffix(".meta").open("w") as f:
            json.dump(meta, f)

    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        # test degenerate cases first
        if start_dt and self._latest < start_dt:
            return []
        if end_dt and end_dt <= self._earliest:
            return []

        start_dt = start_dt or self._earliest
        end_dt = end_dt or self._latest + timedelta(hours=1)  # as to include the last day

        start_date = start_dt.date()
        end_date = end_dt.date()
        one_day = timedelta(days=1)

        ids: list[str] = []
        current_date = start_date
        while current_date <= end_date:
            dir = self._dt2dir(current_date)
            if dir.is_dir():
                for file in sorted(dir.iterdir(), key=lambda file: file.name):
                    # check it's a well-formed id
                    res = FileMessageStore._PARSE_ID.match(file.name)
                    if not res:
                        continue
                    # check its dt is within span
                    ymd_hmsf = res.group("year", "month", "day", "hour", "minute", "second", "microsecond")
                    if start_dt <= datetime(*(int(t or 0) for t in ymd_hmsf)) < end_dt:
                        ids.append(file.name)
            current_date += one_day

        return ids


# TODO: this is specific to the FileMessageStore, remove the top-level alias
DATE_FORMAT = FileMessageStore.DATE_FORMAT


class DatabaseMessageStoreFactory(MessageStoreFactory):
    """A database-backed message store.

    There is a single actual database file (if it is a file) and each
    new store is tables within it.

    The schema is an implementation detail and you must assume it will
    change. See :meth:`_start` for detail anyway.
    """

    def __init__(self, path: str | Path):
        super().__init__()
        # sanity check (Path inherit from PurePath)
        if not isinstance(path, (str, PurePath)):
            raise PypemanConfigError("database message store requires a path")  # pragma: no cover
        self._path = Path(path)  # XXX: maybe it shouldnt be 'Path', cause it could be other values
        # set of the active (between _start and _stop) stores: when it's empty, `self._conn` doesn't exist;
        # this is essentially a reference counter for the connection
        self._active_stores: set[str] = set()

    def _new_store(self, store_id: str) -> MessageStore:
        # there **must not** be any of these char in `store_id`
        # (picked as to avoid all and any escaping need - ie quoting is enough but we're being extra-careful)
        assert not set("\"':?\\[]`") & set(store_id), f"store id {store_id!r} contains annoying characters"
        # see notes in constructor for the parameters
        return DatabaseMessageStore(self, store_id)

    async def _store_started(self, store_id: str):
        """Called by MessageStore when starting.

        When this is the first store, a connection is established.
        """
        if not self._active_stores:
            self._conn = await aiosqlite.connect(self._path)
        self._active_stores.add(store_id)

    async def _store_stopped(self, store_id: str):
        """Called by MessageStore when stopping.

        If this was the last known store, the connection is closed.
        """
        self._active_stores.remove(store_id)
        if not self._active_stores:
            await self._conn.commit()
            await self._conn.close()

    async def execute(self, sql: str, parameters: tuple[Any, ...]):
        """Delegate method. Not private because used in MessageStore."""
        return await self._conn.execute(sql, parameters)

    async def executescript(self, sql_script: str):
        """Delegate method. Not private because used in MessageStore."""
        return await self._conn.executescript(sql_script)

    async def executemany(self, sql: str, parameters: list[tuple[Any, ...]]):
        """Delegate method. Not private because used in MessageStore."""
        return await self._conn.executemany(sql, parameters)

    async def execute_fetchall(self, sql: str, parameters: tuple[Any, ...]):
        """Delegate method. Not private because used in MessageStore."""
        return await self._conn.execute_fetchall(sql, parameters)

    async def commit(self):
        """Delegate method. Not private because used in MessageStore."""
        return await self._conn.commit()

    async def _delete_store(self, store: MessageStore):
        assert isinstance(store, DatabaseMessageStore)  # type narrowing
        # private usage in friend class
        store_id = store._store_id
        if store_id not in self._active_stores:
            return  # store has not been started
        try:
            await self.executescript(
                f"""
 DROP TABLE "{store_id}.entries_ctx";
 DROP TABLE "{store_id}.stored_entries";
 """,
            )
        except OperationalError:
            # because tables are created lazily when stores are started,
            # there is a world in which the tables do not exist; we accept
            # this as a normal condition
            pass


class DatabaseMessageStore(MessageStore):
    def __init__(self, factory_handle: DatabaseMessageStoreFactory, store_id: str):
        super().__init__()
        # the factory acts as a proxy to the connection (then to the database)
        self._db = factory_handle
        self._store_id = store_id

    async def _start(self):
        # notify that we are starting; after that only is the connection ensured to exist
        await self._db._store_started(self._store_id)

        await self._db.executescript(
            f"""
 CREATE TABLE IF NOT EXISTS "{self._store_id}.stored_entries" (
     id                   TEXT PRIMARY KEY                                    NOT NULL,
     timestamp            REAL                                                NOT NULL,
     meta                 TEXT                                                NOT NULL,
     -- ^ json, tho we know it should be dict[str, list[str] | str], anything better we can do? thinkin no..
     store_id             TEXT, -- XXX
     store_chan_name      TEXT, -- XXX
     message_payload      TEXT                                                NOT NULL, -- json
     message_meta         TEXT                                                NOT NULL) -- json
 STRICT, WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS "{self._store_id}.entries_ctx" (
     name                 TEXT                                                NOT NULL,
     payload              TEXT                                                NOT NULL, -- json
     meta                 TEXT                                                NOT NULL, -- json
     ctx_of               TEXT                                                NOT NULL,
     FOREIGN KEY(ctx_of)  REFERENCES "{self._store_id}.stored_entries"(id),
     PRIMARY KEY(ctx_of, name))
 STRICT, WITHOUT ROWID;
 """,
        )

    async def _stop(self):
        # notify that we are stopping and will not be using the connection anymore
        await self._db._store_stopped(self._store_id)

    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        try:
            await self._db.execute(
                f'INSERT INTO "{self._store_id}.stored_entries" VALUES (?, ?, ?, ?, ?, ?, ?)',
                (
                    msg.uuid,
                    msg.timestamp.timestamp(),
                    json.dumps(ini_meta),
                    msg.store_id,
                    msg.store_chan_name,
                    json.dumps(msg.payload),
                    json.dumps(msg.meta),
                ),
            )
            await self._db.executemany(
                f'INSERT INTO "{self._store_id}.entries_ctx" VALUES (?, ?, ?, ?)',
                [
                    (name, json.dumps(thingy["payload"]), json.dumps(thingy["meta"]), msg.uuid)
                    for name, thingy in msg.ctx.items()
                ],
            )
            await self._db.commit()

        except IntegrityError as e:
            raise ValueError(*e.args)
        return msg.uuid

    async def _delete(self, id: str):
        # no error when `id` didn't exist..
        await self._db.execute(f'DELETE FROM "{self._store_id}.entries_ctx" WHERE ? = ctx_of', (id,))
        await self._db.execute(f'DELETE FROM "{self._store_id}.stored_entries" WHERE ? = id', (id,))

    async def _total(self) -> int:
        res = await self._db.execute_fetchall(f'SELECT count(*) FROM "{self._store_id}.stored_entries"', ())
        return res[0][0]

    async def _get_message(self, id: str) -> Message:
        ctx_bits = await self._db.execute_fetchall(
            f'SELECT name, payload, meta FROM "{self._store_id}.entries_ctx" WHERE ? = ctx_of',
            (id,),
        )

        msg_bits = await self._db.execute_fetchall(
            f"""
 SELECT timestamp, store_id, store_chan_name, message_payload, message_meta
 FROM "{self._store_id}.stored_entries" WHERE ? = id
 """,
            (id,),
        )

        if not msg_bits:
            raise LookupError(f"no message stored under {id!r}")
        timestamp, store_id, store_chan_name, message_payload, message_meta = msg_bits[0]

        msg = Message()
        msg.timestamp = datetime.fromtimestamp(timestamp)
        msg.uuid = id
        msg.payload = json.loads(message_payload)
        msg.meta = json.loads(message_meta)
        msg.store_id = store_id
        msg.store_chan_name = store_chan_name
        msg.ctx = {
            name: {"payload": json.loads(payload), "meta": json.loads(meta)}
            for name, payload, meta in ctx_bits
        }
        return msg

    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        res = await self._db.execute_fetchall(
            f'SELECT meta FROM "{self._store_id}.stored_entries" WHERE ? = id',
            (id,),
        )
        if not res:
            raise LookupError(f"no message stored under {id!r}")
        return json.loads(res[0][0])

    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        res = await self._db.execute_fetchall(
            f'UPDATE "{self._store_id}.stored_entries" SET meta = ? WHERE ? = id RETURNING id',
            (json.dumps(meta), id),
        )
        if not res:
            raise LookupError(f"no message stored under {id!r}")
        await self._db.commit()

    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        ids = await self._db.execute_fetchall(
            f'SELECT id FROM "{self._store_id}.stored_entries" WHERE ? <= timestamp AND timestamp < ?',
            (
                0 if start_dt is None else start_dt.timestamp(),
                # hopefully pypeman burns before 2286!
                10**10 if end_dt is None else end_dt.timestamp(),
            ),
        )
        return [id for id, in ids]
