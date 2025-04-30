from __future__ import annotations

import asyncio
import json
import re
from abc import ABC
from abc import abstractmethod
from copy import deepcopy
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from pathlib import PurePath
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Literal
from typing import TypedDict

from dateutil import parser as dateutilparser

from pypeman.errors import PypemanConfigError
from pypeman.message import Message

if TYPE_CHECKING:  # pragma: no cover
    from typing import override
else:

    def override(f):
        return f


"""
class Message:
    State_ = Literal["wait_retry", "pending", "processing", "processed", "rejected", "error"]
    timestamp: datetime
    uuid: str
    payload: Any
    meta: dict[str, Any]

    PENDING = "pending"

    def to_dict(self, encode_payload: bool = True) -> dict[str, Any]: ...
    def to_json(self) -> str: ...
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Message: ...
    @staticmethod
    def from_json(data: str) -> Message: ...
    # """


# base classes {{{1
# factory {{{2
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
        await self._delete_store(existing)
        # private usage in friend class
        existing._cached_total = 0


# store {{{2
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

        id: str  # store-dependant id, different from message.uuid
        meta: dict[str, Any]  # store-related meta, different from message.meta
        message: Message
        state: Message.State_  # TODO: remove in favor of _['meta']['state']
        # timestamp: datetime  # TODO: remove in favor of message.timestamp if equivalent

    async def _start(self):
        """(implementation) Called at startup to initialize the store.

        This method is not marked as abstract and thus not required.
        """

    @abstractmethod
    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        """(implementation) Store a message.

        Implementation must ensure that all operations with an id
        obtained this way are valid until a matching :meth:`_delete`.

        :param msg: Message to be stored.
        :param ini_meta: Store-related initial meta.
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

    async def store(self, msg: Message) -> str:
        """Store a message in the store.

        Its state is right away initialized to :obj:`Message.PENDING`.

        :param msg: Message to store.
        :return: Id for this specific message.
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
            "timestamp": <datetime.now()>
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
                "timestamp": datetime.now(),
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
        return (await self.get(id))["message"]

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

            * `text_<name>`: string to search in meta value;
            * `rtext_<name>`: string regex to search in meta value;

            * `start_<name>` / `end_<name>`: filter a range, values are
                interpreted and compared as numbers when possible;

            * `<name>`: only match with exact value;

            Reminder: when called through web API, ie from `list_msgs`
            in pypeman/plugins/remoteadmin/views.py, these keys, in the
            request params, are prefixed with "meta_" (eg.
            "meta_state" "meta_rtext_url") but here they no longer are.

        :return: List of fitting message entries or, if `group_by` is
            given, dict of group to list of fitting message entries.
        """
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
            return filtered

        group = meta_filt.group if group_by_meta else payload_filt.group
        grouped: dict[str, list[MessageStore.StoredEntry_]] = {}
        for it in filtered:
            # at this point filtered is also sorted, so each individual group will be too
            grouped.setdefault(group(it), []).append(it)
        return grouped


# filtering and sorting {{{2
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
                filt = getattr(_MetaFilt._FILTERS, filt_name, _MetaFilt._FILTERS.exact)
                self.filters.setdefault(meta_name, []).append(filt(value))

    def filter(self, entry: MessageStore.StoredEntry_) -> bool:
        """:return: true if the entry is to be kept in the result

        every filter on meta must match, but on any info of the meta
        """
        meta = entry["meta"]
        # note that no filter will return True as expected
        for name, filts in self.filters.items():
            m = meta.get(name, [])
            info = m if isinstance(m, list) else [m]
            for filt in filts:  # .             for each filter for this meta info,
                if not any(map(filt, info)):  # if none of the values matche
                    return False  # .           then stop here
            # the lines above are roughly equivalent to:
            # ( filts[0](info[0]) OR filts[0](info[1]) OR .. ) AND ( filts[1](info[0]) OR .. ) AND ..
        return True

    def order(self, entry: MessageStore.StoredEntry_) -> Any:
        """:return: :class:`str` of the meta

        uses the first value if the meta is a list
        for an empty list will use an empty string
        same if this meta is not present
        """
        m = entry["meta"].get(self.order_by, "")
        return str(m if not isinstance(m, list) else m[0] if len(m) else "")

    def group(self, entry: MessageStore.StoredEntry_) -> str:
        """:return: group name for entry

        uses the first value if the meta is a list
        for an empty list will use an empty string
        same if this meta is not present

        TODO(potential evolution): if deemed interesting,
            messages could be present in multiple groups
        """
        m = entry["meta"].get(self.group_by, "")
        return str(m if not isinstance(m, list) else m[0] if len(m) else "")


# implementations {{{1
# null/noop {{{2
class NullMessageStoreFactory(MessageStoreFactory):  # pragma: no cover
    """A no-op message store.

    Always successful, nothing is stored. I don't know what that
    implies, it's scary, but it's needed.

    That is: storage/update/delete operations are always successful,
    however retrieve operations do not return anything relevant.
    Mostly, retrieved messages will be :obj:`NotImplemented`.
    """

    @override
    def _new_store(self, store_id: str) -> MessageStore:
        return NullMessageStore()

    @override
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

    @override
    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        # non-empty so it doesn't compare falsey and reflects its origin, just in case
        return "nullstore-key"

    @override
    async def _delete(self, id: str):
        pass

    @override
    async def _total(self) -> int:
        return 0

    @override
    async def _get_message(self, id: str) -> Message:
        return NotImplemented

    @override
    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        return {}

    @override
    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        pass

    @override
    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        return []


# memory {{{2
class MemoryMessageStoreFactory(MessageStoreFactory):
    """An in-memory message store.

    All messages are lost at pypeman stop.
    """

    @override
    def _new_store(self, store_id: str) -> MessageStore:
        return MemoryMessageStore()

    @override
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
        # this is a list so that it can be sort-inserted by timestamp
        self._sorted: list[MemoryMessageStore.MemoryStoredEntry_] = []
        # accompanying mapping for direct accesses by id
        self._mapped: dict[str, MemoryMessageStore.MemoryStoredEntry_] = {}
        # 2 references are stored to the same entry for 2 different usage

    @override
    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        if msg.uuid in self._mapped:
            return msg.uuid

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

    @override
    async def _delete(self, id: str):
        msg = self._mapped.pop(id)
        self._sorted.remove(msg)

    @override
    async def _total(self) -> int:
        return len(self._sorted)

    @override
    async def _get_message(self, id: str) -> Message:
        return Message.from_dict(self._mapped[id]["message"])

    @override
    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        return deepcopy(self._mapped[id]["meta"])

    @override
    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        self._mapped[id]["meta"] = meta

    @override
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


# file {{{2
class FileMessageStoreFactory(MessageStoreFactory):
    """A filesystem-based message store.

    Messages are stored in file pairs under
    `<base_path>/<store_id>/<year>/<month>/<day>/`:
        * `<timestamp>_<uuid>.meta`  the store-related meta;
        * `<timestamp>_<uuid>`       the actual message.

    The `<timestamp>` part is as :obj:`FileMessageStore.DATE_FORMAT`.
    """

    # TODO: see if both the parameter and the property names can be harmonized
    def __init__(self, path: str | PurePath):
        super().__init__()
        # sanity check
        if not isinstance(path, (str, PurePath)):
            raise PypemanConfigError("file message store requires a path")  # pragma: no cover
        self.base_path = PurePath(path)

    @override
    def _new_store(self, store_id: str) -> MessageStore:
        # there **must not** be any of these char in `store_id`
        assert not set(r"\./") & set(store_id)
        return FileMessageStore(self.base_path, store_id)

    @override
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

        rmrf(store.base_path)


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
        # TODO: see if property name and parameter names can be changed/harmonized
        self.base_path = Path(path, store_id)
        self.base_path.mkdir(parents=True, exist_ok=True)

    @override
    async def _start(self):
        # in _start rather than __init__ for sematics and potential async rewrite
        self._earliest = self._extreme(latest=False)
        self._latest = self._extreme(latest=True)

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

    @override
    async def _store(self, msg: Message, ini_meta: dict[str, Any]) -> str:
        id = f"{msg.timestamp.strftime(FileMessageStore.DATE_FORMAT)}_{msg.uuid}"
        # at time of writing :class:`Message` isn't correctly typed (*at all)
        # these two lines are to enforce correct type (the aliase can be removed when updated)
        timestamp: ... = msg.timestamp
        timestamp: datetime
        msg_path = self._dt2dir(timestamp) / id

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

    @override
    async def _delete(self, id: str):
        msg_path = self._id2file(id)
        msg_path.unlink(missing_ok=True)
        msg_path.with_suffix(".meta").unlink(missing_ok=True)

    @override
    async def _total(self) -> int:
        return sum(
            file.is_file() and FileMessageStore._PARSE_ID.match(file.name) is not None
            for file in self.base_path.glob("*/*/*/*")  # <y>/<m>/<d>/<file>
        )

    @override
    async def _get_message(self, id: str) -> Message:
        msg_path = self._id2file(id)
        if not msg_path.exists():
            raise LookupError(f"no message stored under {id!r}")
        with msg_path.open("r") as f:
            return Message.from_json(f.read())

    @override
    async def _get_storemeta(self, id: str) -> dict[str, Any]:
        meta_path = self._id2file(id).with_suffix(".meta")
        if not meta_path.exists():
            raise LookupError(f"no meta stored under {id!r}")
        with meta_path.open("r") as f:
            content = f.read()
        # (uuuuuuugh) still handle oldass format
        return json.loads(content) if "{" == content[:1] else {"state": content}

    @override
    async def _set_storemeta(self, id: str, meta: dict[str, Any]):
        with self._id2file(id).with_suffix(".meta").open("w") as f:
            json.dump(meta, f)

    def _extreme(self, latest: bool) -> datetime:
        """Find the earliest/latest stored message `dt`.

        This method has the same status as :meth:`_total`: it gets the
        value form the source and so should be called only if truly
        needed, otherwise :attr:`_earliest`/:attr:`_latest` should be
        sufficient. Not marked async but could be at some point.
        This method is implementation detail.

        :param latest: False to find earliest, True to find latest.
        """
        # quick guard check in case there are no message at all
        if not self.base_path.is_dir() or not next(self.base_path.iterdir(), False):
            return datetime.min if latest else datetime.max

        # if looking for latest, start with smallest values
        if latest:
            extrms_ymd = ["0001", "01", "01"]
            extrms_hmsf = ("00", "00", "00", "000000")
            if_no_sec_usec = ("59", "999999")
            cmp = lambda cur, extrm: extrm < cur  # noqa: E731
        else:
            extrms_ymd = ["9999", "12", "31"]
            extrms_hmsf = ("23", "59", "59", "999999")
            if_no_sec_usec = ("00", "000000")
            cmp = lambda cur, extrm: cur < extrm  # noqa: E731
        cmp: Callable[[Any, Any], bool]

        # iter over year/month/day to find each min
        # `cmp(..)` works because components in the path are all fixed-width
        # (ie str comparison is enough, no need to parse)
        path_accu = self.base_path
        for k in range(len(extrms_ymd)):
            for it in path_accu.iterdir():
                if cmp(it.name, extrms_ymd[k]):
                    extrms_ymd[k] = it.name
            path_accu /= extrms_ymd[k]
        # now `path_accu` is `'<base>/<y>/<m>/<d>/'`

        for id in path_accu.iterdir():
            res = FileMessageStore._PARSE_ID.match(id.name)
            if res:
                gd = res.groupdict()
                hmsf = (
                    gd["hour"],
                    gd["minute"],
                    gd.get("second", if_no_sec_usec[0]),
                    gd.get("microsecond", if_no_sec_usec[1]),
                )
                if cmp(hmsf, extrms_hmsf):
                    extrms_hmsf = hmsf

        Y, m, d = map(int, extrms_ymd)
        H, M, S, f = map(int, extrms_hmsf)
        return datetime(Y, m, d, H, M, S, f)

    @override
    async def _span_select(self, start_dt: datetime | None, end_dt: datetime | None) -> list[str]:
        # test degenerate cases first
        if start_dt and self._latest < start_dt:
            return []
        if end_dt and end_dt <= self._earliest:
            return []

        start_dt = start_dt or self._earliest
        end_dt = end_dt or self._latest + timedelta(hours=1)  # as to include the last one

        start_day = start_dt.date()
        end_day = end_dt.date()
        one_day = timedelta(days=1)

        ids: list[str] = []
        day = start_day
        while day <= end_day:
            dir = self._dt2dir(day)
            if dir.is_dir():
                for file in dir.iterdir():
                    # check it's a well-formed id
                    res = FileMessageStore._PARSE_ID.match(file.name)
                    if not res:
                        continue
                    # check its dt is within span
                    ymd_hmsf = res.group("year", "month", "day", "hour", "minute", "second", "microsecond")
                    if start_dt <= datetime(*(int(t or 0) for t in ymd_hmsf)) < end_dt:
                        ids.append(file.name)
            day += one_day

        return ids


# TODO: this is specific to the FileMessageStore, remove the top-level alias
DATE_FORMAT = FileMessageStore.DATE_FORMAT
