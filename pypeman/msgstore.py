import datetime
import dateutil.parser
import json
import logging
import os
import re
import time

from itertools import islice
from collections import OrderedDict
from pathlib import Path

from pypeman.message import Message

from pypeman.errors import PypemanConfigError

logger = logging.getLogger("pypeman.store")

DATE_FORMAT = '%Y%m%d_%H%M%S%f'


class MessageStoreFactory():
    """ Message store factory class can generate Message store instance for specific store_id. """

    def get_store(self, store_id):
        """
        :param store_id: identifier of corresponding message store.
        :return: A MessageStore corresponding to correct store_id.
        """


class MessageStore():
    """ A MessageStore keep an history of processed messages. Mainly used in channels. """

    async def start(self):
        """
        Called at startup to initialize store.
        """

    async def store(self, msg):
        """
        Store a message in the store.

        :param msg: The message to store.
        :return: Id for this specific message.
        """

    async def add_message_meta_infos(self, id, meta_info_name, info):
        """
        Add message meta infos in the store

        :param id: Message specific store id.
        :param meta_info_name: The name of the meta info to create/update
        :param info: The info value
        """

    async def get_message_meta_infos(self, id, meta_info_name=None):
        """
        Get message meta infos in the store

        :param id: Message specific store id.
        :param meta_info_name: (optional) The name of the meta info to get,
            if not set returns the entire dict
        """

    async def change_message_state(self, id, new_state):
        """
        Change the `id` message state.

        :param id: Message specific store id.
        :param new_state: Target state.
        """

    async def add_sub_message_state(self, id, sub_id, state):
        """
        Add a state to the meta 'submessages_state_history" of a message
        submessages_state_history meta is a list of dicts of this form:
        {
            "sub_id": <sub_id>,
            "state": <state>,
            "timestamp": <time.time()>
        }

        :param id: Message specific store id.
        :param sub_id: The sub message's id (could be the same as the id)
        :param state: Target state.
        """
        try:
            submessages_state_history = await self.get_message_meta_infos(
                id=id,
                meta_info_name="submessages_state_history"
            )
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
        await self.add_message_meta_infos(
            id=id,
            meta_info_name="submessages_state_history",
            info=submessages_state_history,
        )

    async def set_state_to_worst_sub_state(self, id):
        """
        Change the `id` message state to the worst state stored in submessages_state_history

        :param id: Message specific store id.
        """
        try:
            submessages_state_history = await self.get_message_meta_infos(
                id=id,
                meta_info_name="submessages_state_history"
            )
        except KeyError:
            submessages_state_history = []
        if not submessages_state_history:
            raise IndexError("No sub message state stored, cannot choose the worst")
        worst_state = submessages_state_history[0]["state"]
        for submsg_info in submessages_state_history:
            state = submsg_info["state"]
            if Message.STATES_PRIORITY.index(worst_state) < Message.STATES_PRIORITY.index(state):
                worst_state = state
        await self.change_message_state(id=id, new_state=worst_state)

    async def get(self, id):
        """
        Return one message corresponding to given `id` with his status.

        :param id: Message id. Message store dependant.
        :return: A dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """

    async def get_preview_str(self, id):
        """
        Return the first 1000 chars of message content corresponding to `id`

        :param id: Message id. Message store dependant.
        :return: A dict `{'id':<message_id>, 'message_content': str}`.
        """

    async def get_msg_content(self, id):
        """
        Return the content of the message corresponding to given `id`

        :param id: Message id. Message store dependant.
        :return: A dict `{'id':<message_id>, 'message_content': Message}`.
        """

    async def is_regex_in_msg(self, id, rtext):
        """
        Return True if the str(msg) contains the regex rtext

        :param id: Message id. Message store dependant.
        :param rtext: string of regular expression to search in msg
        :return: True if it matches False otherwise
        """

    async def is_txt_in_msg(self, id, text):
        """
        Return True if the str(msg) contains param text

        :param id: Message id. Message store dependant.
        :param text: String. The text to search in msg
        :return: True if it text is found, False otherwise
        """

    async def search(self, start=0, count=10, order_by='timestamp', start_dt=None, end_dt=None,
                     text=None, rtext=None, start_id=None, meta=None):
        """
        Return a list of message with store specific `id` and processed status.

        :param start: (DEPRECATED: use start_id instead) First element.
        :param count: Count of elements since first element.
        :param order_by: Message order. Allowed values : ['timestamp', 'status'].
        :param start_dt: (optional) Isoformat start date(time) to filter with
        :param end_dt: (optional) Isoformat end date(time) to filter with
        :param text: (optional) String to search in message content
        :param rtext: (optional) String regex to search in message content
        :param start_id: (optional): If set, start search from this id (excluded from rslt list)
        :param meta: (optional): Search filters applied to message metadata

            Message metadata are always stored as a list of strings when
            going through `BaseNode.store_meta`. (If an in-store entry is not
            a list, it will be handled as if a list of a single item.) If any
            value from the inspected list of meta correspond to the query,
            the message is selected.

            In the key/value pairs of the `meta` argument to `search`,
            keys are processed as follow:

            * `text_<name>`: string to search in meta value.
            * `rtext_<name>`: string regex to search in meta value.

            * `start_<name>` / `end_<name>`: filter a range; values are
                interpreted and compared as numbers when possible.

            * `<name>`: only match with exact value.

            * `order_by`: sort result by the meta indicated by value.

            Reminder: when called through web API, ie from `list_msgs` in pypeman/plugins/remoteadmin/views.py
            these keys are prefixed with "meta_", eg "meta_status" "meta_rtext_url".

        :return: A list of dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """

    @staticmethod
    def _search_meta_filter_sort(meta_search: dict, results: list):
        """for `meta_search`, see `meta` in the class' `search`"""
        def isfloat(s: str):
            """used with start_/end_"""
            try:
                float(s)
                return True
            except ValueError:
                return False

        class _FILTERS:
            @staticmethod
            def exact(value: str):
                return lambda info: info == value

            @staticmethod
            def text(text: str):
                return lambda info: text in info

            @staticmethod
            def rtext(rtext: str):
                regex = re.compile(rtext)
                return lambda info: bool(regex.search(info))

            @staticmethod
            def start(start: str):
                fstart = float(start)
                return lambda info: isfloat(info) and float(info) >= fstart

            @staticmethod
            def end(end: str):
                fend = float(end)
                return lambda info: isfloat(info) and float(info) <= fend

        # list of tuples (meta_info_name, filter_function)
        filters: "list[tuple[str, type[bool]]]" = []
        ordering = None
        for key, value in meta_search.items():
            if key.startswith('order_by'):
                ordering = value
            else:
                filt_name, _, meta_name = key.partition('_')
                filt = getattr(_FILTERS, filt_name, _FILTERS.exact)
                filters.append((meta_name, filt(value)))

        filtered = (
            # For an item to be kept, ..
            item for item in results
            # .. it must pass all filters.
            if all(
                # The meta must exists, else message is filtered out.
                meta_name in item['message'].meta
                and (
                    # Any info in the list may pass, one is enough.
                    any(filt(info) for info in item['message'].meta[meta_name])
                    # Info stored through `BaseNode.store_meta` are list[str] ..
                    if isinstance(item['message'].meta[meta_name], list)
                    # .. but they might not be if added through an other mean.
                    else filt(str(item['message'].meta[meta_name]))
                )
                for meta_name, filt in filters
            )
        )

        if ordering is None:
            return list(filtered)

        def key(item):
            info = item['message'].meta.get(meta_name)
            if not info:  # None, empty list, missing, ..
                return ""
            return str(info[0] if isinstance(info, list) else info)

        # if starting with a '-', reverse ordering, the meta_name is the rest
        meta_name = ordering[ordering[0] == '-':]
        return sorted(filtered, key=key, reverse=ordering[0] == '-')

    async def total(self):
        """
        :return: total count of messages
        """

    async def delete(self, id):
        """
        Delete one message in the store corresponding to given `id` with his status.
        Useful for tests and maybe in the future to permits cleanup of message store

        !CAUTION! : cannot be undone

        :param id: Message id. Message store dependant.
        :return: A dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """


class NullMessageStoreFactory(MessageStoreFactory):
    """ Return an NullMessageStore that do nothing at all. """
    def get_store(self, store_id):
        return NullMessageStore()


class NullMessageStore(MessageStore):
    """ For testing purpose """

    async def store(self, msg):
        return None

    async def get(self, id):
        return None

    async def add_message_meta_infos(self, id, meta_info_name, info):
        return None

    async def get_message_meta_infos(self, id, meta_info_name=None):
        return None if meta_info_name else {}

    async def add_sub_message_state(self, id, sub_id, state):
        return None

    async def set_state_to_worst_sub_state(self, id):
        return None

    async def get_preview_str(self, id):
        return None

    async def change_message_state(self, id, new_state):
        return None

    async def generate_store_id_from_msg(self, msg):
        return None

    async def get_msg_content(self, id):
        return None

    async def search(self, **kwargs):
        return None

    async def total(self):
        return 0

    async def delete(self, id):
        return None


class FakeMessageStoreFactory(MessageStoreFactory):
    """ Return an Fake message store """
    def get_store(self, store_id):
        return FakeMessageStore()


class FakeMessageStore(MessageStore):
    """ For testing purpose """

    async def store(self, msg):
        logger.debug("Should store message %s", msg)
        return 'fake_id'

    async def add_message_meta_infos(self, id, meta_info_name, info):
        return None

    async def get_message_meta_infos(self, id, meta_info_name=None):
        return "processed" if meta_info_name else {}

    async def get(self, id):
        return {'id': id, 'state': 'processed', 'message': None}

    async def get_preview_str(self, id):
        return {"id": id, "message_content": "content"}

    async def get_msg_content(self, id):
        return {"id": id, "message_content": "content"}

    async def is_regex_in_msg(self, id, rtext):
        return True

    async def is_txt_in_msg(self, id, text):
        return True

    async def change_message_state(self, id, new_state):
        return None

    async def add_sub_message_state(self, id, sub_id, state):
        return None

    async def set_state_to_worst_sub_state(self, id):
        return None

    async def search(self, **kwargs):
        return []

    async def total(self):
        return 0

    async def delete(self, id):
        """
            we delete nothing here, but return what would have been deleted
            (for testing)
        """
        return {'id': id, 'state': 'processed', 'message': None}


class MemoryMessageStoreFactory(MessageStoreFactory):
    """ Return a Memory message store. All message are lost at pypeman stop. """
    def __init__(self):
        self.base_dict = {}

    def get_store(self, store_id):
        return MemoryMessageStore(self.base_dict, store_id)


class MemoryMessageStore(MessageStore):
    """ Store messages in memory """

    def __init__(self, base_dict, store_id):
        super().__init__()
        self.messages = base_dict.setdefault(store_id, OrderedDict())

    async def store(self, msg):
        msg_id = msg.uuid
        self.messages[msg_id] = {
            'id': msg_id, 'state': Message.PENDING,
            'timestamp': msg.timestamp, 'message': msg.to_dict()}
        return msg_id

    async def change_message_state(self, id, new_state):
        await self.add_message_meta_infos(id, "state", new_state)

    async def add_message_meta_infos(self, id, meta_info_name, info):
        self.messages[id][meta_info_name] = info

    async def get_message_meta_infos(self, id, meta_info_name=None):
        return self.messages[id][meta_info_name] if meta_info_name else self.messages[id]

    async def get(self, id):
        resp = dict(self.messages[id])
        resp['message'] = Message.from_dict(resp['message'])
        return resp

    async def get_preview_str(self, id):
        msg = await self.get_msg_content(id)
        try:
            msg.payload = str(msg.payload)[:1000]
        except Exception:
            msg.payload = repr(msg.payload)[:1000]
        return msg

    async def get_msg_content(self, id):
        msg = await self.get(id)
        msg_content = msg["message"]
        return msg_content

    async def is_regex_in_msg(self, id, rtext):
        msg = await self.get_msg_content(id)
        try:
            msg.payload = str(msg.payload)
        except Exception:
            msg.payload = repr(msg.payload)
        regex = re.compile(rtext)
        return True if regex.match(msg.payload) else False

    async def is_txt_in_msg(self, id, text):
        msg = await self.get_msg_content(id)
        try:
            msg.payload = str(msg.payload)
        except Exception:
            msg.payload = repr(msg.payload)
        return text in msg.payload

    async def search(self, start=0, count=10, order_by='timestamp', start_dt=None, end_dt=None,
                     text=None, rtext=None, start_id=None, meta=None):
        if start and start_id:
            raise ValueError("`start` and `start_id` can't both be set")
        if order_by.startswith('-'):
            reverse = True
            sort_key = order_by[1:]
        else:
            reverse = False
            sort_key = order_by

        if start_dt:
            start_dt = dateutil.parser.isoparse(start_dt)
        if end_dt:
            end_dt = dateutil.parser.isoparse(end_dt)

        result = []
        values = (
            val for val in self.messages.values()
            if (not start_dt or val["timestamp"] >= start_dt)
            and (not end_dt or val["timestamp"] <= end_dt)
        )
        if text:
            found_values = []
            for val in values:
                if await self.is_txt_in_msg(val["id"], text):
                    found_values.append(val)
            values = found_values

        if rtext:
            found_values = []
            for val in values:
                if await self.is_regex_in_msg(val["id"], rtext):
                    found_values.append(val)
            values = found_values

        ordered_list = sorted(values, key=lambda x: x[sort_key], reverse=reverse)
        if start:
            filtered_iterator = islice(
                ordered_list,
                start, start + count
            )
        elif start_id:
            for idx, msgdict in enumerate(ordered_list):
                if msgdict["id"] == start_id:
                    start_id_idx = idx + 1
                    break
            else:
                raise IndexError("Couldn't find start_id %r in filtered results", start_id)
            filtered_iterator = islice(
                ordered_list,
                start_id_idx, start_id_idx + count
            )
        else:
            filtered_iterator = islice(
                ordered_list,
                0, count
            )
        for value in filtered_iterator:
            resp = dict(value)
            resp['message'] = Message.from_dict(resp['message'])
            result.append(resp)
        return result if meta is None else MessageStore._search_meta_filter_sort(meta, result)

    async def total(self):
        return len(self.messages)

    async def delete(self, id):
        resp = dict(self.messages.pop(id))
        resp['message'] = Message.from_dict(resp['message'])
        return resp


class FileMessageStoreFactory(MessageStoreFactory):
    """
    Generate a FileMessageStore message store instance.
    Store a file in `<base_path>/<store_id>/<year>/<month>/<day>/` hierachy.
    """

    # TODO add an option to reguraly archive old file or delete them
    def __init__(self, path):
        super().__init__()
        if path is None:
            raise PypemanConfigError('file message store requires a path')
        self.base_path = path

    def get_store(self, store_id):
        return FileMessageStore(self.base_path, store_id)


class FileMessageStore(MessageStore):
    """ Store a file in `<base_path>/<store_id>/<year>/<month>/<day>/` hierachy."""
    # TODO file access should be done in another thread. Waiting for file backend.

    def __init__(self, path, store_id):
        super().__init__()

        self.base_path = os.path.join(path, store_id)

        # Match msg file name
        self.old_msg_re = re.compile(
            r'^(?P<msg_date>[0-9]{8})_(?P<msg_time>[0-9]{2}[0-9]{2})_(?P<msg_uid>[0-9a-zA-Z]*)$')
        self.msg_re = re.compile(
            r'^(?P<msg_date>[0-9]{8})_(?P<msg_time>[0-9]{2}[0-9]{2}[0-9]{2}[0-9]{6})_(?P<msg_uid>[0-9a-zA-Z]*)$'  # noqa: E501
        )

        try:
            # Try to make dirs if necessary
            os.makedirs(os.path.join(self.base_path))
        except FileExistsError:
            pass

        self._total = 0

    def id2path(self, id):
        old_match = self.old_msg_re.match(id)
        match = self.msg_re.match(id)
        if not (match or old_match):
            raise ValueError(f"Id '{id}' not a correct id")
        if old_match:
            match = old_match
        msg_str_date = match.groupdict()["msg_date"]
        year = msg_str_date[:4]
        month = msg_str_date[4:6]
        day = msg_str_date[6:8]
        msg_path = Path(self.base_path) / year / month / day / id
        return msg_path

    async def start(self):
        self._total = await self.count_msgs()

    async def store(self, msg):
        """ Store a file in `<base_path>/<store_id>/<month>/<day>/` hierachy."""
        # TODO implement a safer store to avoid broken messages

        # The filename is the file id
        filename = "{}_{}".format(msg.timestamp.strftime(DATE_FORMAT), msg.uuid)
        msg_path = self.id2path(filename)
        msg_path.parent.mkdir(parents=True, exist_ok=True)

        # Write message to file
        with msg_path.open("w") as f:
            f.write(msg.to_json())

        await self.change_message_state(filename, Message.PENDING)

        self._total += 1

        return filename

    def _is_json_meta(self, id):
        """Check if the message meta file is a json. If it's not,
        the message is an old message that doesnt contain other infos in
        meta except the state

        Args:
            id (str): The id of the message
        """
        msg_fpath = self.id2path(id)
        meta_fpath = msg_fpath.with_suffix(".meta")
        if not meta_fpath.exists():
            return False
        with meta_fpath.open("r") as fin:
            try:
                json.load(fin)
                return True
            except json.JSONDecodeError:
                return False

    def _convert_meta_to_json(self, id):
        """Convert an old message meta to a new one (json)

        Args:
            id (str): The id of the message to convert
        """
        msg_fpath = self.id2path(id)
        meta_fpath = msg_fpath.with_suffix(".meta")
        with meta_fpath.open("r") as fin:
            state = fin.read()
        meta_data = {"state": state}
        with meta_fpath.open("w") as fout:
            json.dump(meta_data, fout)
        return meta_data

    async def get_message_meta_infos(self, id, meta_info_name=None):
        msg_fpath = self.id2path(id)
        meta_fpath = msg_fpath.with_suffix(".meta")
        if meta_fpath.exists():
            is_new_meta_file = self._is_json_meta(id)
            if not is_new_meta_file:
                meta_data = self._convert_meta_to_json(id)
            else:
                with meta_fpath.open("r") as fin:
                    meta_data = json.load(fin)
        else:
            meta_data = {}

        if meta_info_name:
            meta_data = meta_data.get(meta_info_name)
        return meta_data

    async def add_message_meta_infos(self, id, meta_info_name, info):
        msg_fpath = self.id2path(id)
        meta_fpath = msg_fpath.with_suffix(".meta")
        if meta_fpath.exists():
            is_new_meta_file = self._is_json_meta(id)
            if not is_new_meta_file:
                meta_data = self._convert_meta_to_json(id)
            else:
                with meta_fpath.open("r") as fin:
                    meta_data = json.load(fin)
        else:
            meta_data = {}

        meta_data[meta_info_name] = info
        with meta_fpath.open("w") as fout:
            json.dump(meta_data, fout)

    async def change_message_state(self, id, new_state):
        await self.add_message_meta_infos(id, "state", new_state)

    async def get_message_state(self, id):
        return await self.get_message_meta_infos(id, "state")

    async def get(self, id):
        fpath = self.id2path(id)
        if not fpath.exists():
            raise IndexError

        with fpath.open("rb") as f:
            msg = Message.from_json(f.read().decode('utf-8'))
            return {
                'id': id,
                'state': await self.get_message_state(id),
                'message': msg,
                "meta": await self.get_message_meta_infos(id)
            }

    async def get_preview_str(self, id):
        msg = await self.get_msg_content(id)
        try:
            msg.payload = str(msg.payload)[:1000]
        except Exception:
            msg.payload = repr(msg.payload)[:1000]
        return msg

    async def get_msg_content(self, id):
        msg = await self.get(id)
        msg_content = msg["message"]
        return msg_content

    async def sorted_list_directories(self, path, reverse=True):
        """
        :param path: Base path
        :param reverse: reverse order
        :return: List of directories in specified path ordered
        """
        return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))], reverse=reverse)

    async def count_msgs(self):
        """
        Count message by listing all directories. To be used at startup.
        """
        count = 0
        for year in await self.sorted_list_directories(os.path.join(self.base_path)):
            for month in await self.sorted_list_directories(os.path.join(self.base_path, year)):
                for day in await self.sorted_list_directories(os.path.join(self.base_path, year, month)):
                    for msg_id in sorted(os.listdir(os.path.join(self.base_path, year, month, day))):
                        old_found = self.old_msg_re.match(msg_id)
                        found = self.msg_re.match(msg_id)
                        if found or old_found:
                            count += 1
        return count

    async def is_regex_in_msg(self, id, rtext):
        msg = await self.get_msg_content(id)
        try:
            msg.payload = str(msg.payload)
        except Exception:
            msg.payload = repr(msg.payload)
        regex = re.compile(rtext)
        return True if regex.match(msg.payload) else False

    async def is_txt_in_msg(self, id, text):
        msg = await self.get_msg_content(id)
        try:
            msg.payload = str(msg.payload)
        except Exception:
            msg.payload = repr(msg.payload)
        return text in msg.payload

    async def search(self, start=0, count=10, order_by='timestamp', start_dt=None, end_dt=None,
                     text=None, rtext=None, start_id=None, meta=None):
        if start and start_id:
            raise ValueError("`start` and `start_id` can't both be set")
        # TODO better performance for slicing by counting file in dirs ?
        if order_by.startswith('-'):
            reverse = True
            # sort_key = order_by[1:]
        else:
            reverse = False
            # sort_key = order_by

        if start_dt:
            start_dt = dateutil.parser.isoparse(start_dt)
        if end_dt:
            end_dt = dateutil.parser.isoparse(end_dt)

        # TODO handle sort_key
        result = []
        end = start + count
        position = 0
        start_id_found = False
        for year in await self.sorted_list_directories(
                os.path.join(self.base_path), reverse=reverse):
            for month in await self.sorted_list_directories(
                    os.path.join(self.base_path, year), reverse=reverse):
                for day in await self.sorted_list_directories(
                        os.path.join(self.base_path, year, month), reverse=reverse):
                    # Pre filter with date to avoid listing unuseful dirs
                    msg_date = datetime.date(year=int(year), month=int(month), day=int(day))
                    if start_dt:
                        if msg_date < start_dt.date():
                            continue
                    if end_dt:
                        if msg_date > end_dt.date():
                            continue
                    for msg_id in sorted(
                            os.listdir(os.path.join(
                                self.base_path, year, month, day)),
                            reverse=reverse):
                        if not start_id_found and start_id and msg_id != start_id:
                            continue
                        elif not start_id_found and start_id and msg_id == start_id:
                            start_id_found = True
                            continue
                        old_found = self.old_msg_re.match(msg_id)
                        found = self.msg_re.match(msg_id)
                        if found or old_found:
                            if old_found:
                                msg_str_time = found.groupdict()["msg_time"]
                                hour = int(msg_str_time[:2])
                                minute = int(msg_str_time[2:4])
                                second = 0
                                microsecond = 0
                            if found:
                                msg_str_time = found.groupdict()["msg_time"]
                                hour = int(msg_str_time[:2])
                                minute = int(msg_str_time[2:4])
                                second = int(msg_str_time[4:6])
                                microsecond = int(msg_str_time[6:12])
                            msg_time = datetime.time(
                                hour=hour,
                                minute=minute,
                                second=second,
                                microsecond=microsecond,
                            )
                            msg_dt = datetime.datetime.combine(msg_date, msg_time)
                            if start_dt:
                                if msg_dt < start_dt:
                                    continue
                            if end_dt:
                                if msg_dt > end_dt:
                                    continue
                            if text:
                                if not await self.is_txt_in_msg(msg_id, text):
                                    continue
                            if rtext:
                                if not await self.is_regex_in_msg(msg_id, rtext):
                                    continue
                            if start <= position < end:
                                # TODO: need to do processing of payload
                                #       before filtering (HL7 / json-str)
                                # TODO: add filter here
                                # TODO: can we transfoer into a generator?
                                result.append(await self.get(msg_id))
                            elif position >= end:
                                break
                            position += 1
        if start_id and not start_id_found:
            raise IndexError("Couldn't find start_id %r in filtered results", start_id)
        return result if meta is None else MessageStore._search_meta_filter_sort(meta, result)

    async def total(self):
        return self._total

    async def _delete_meta_file(self, id):
        msg_path = self.id2path(id)
        meta_fpath = msg_path.with_suffix(".meta")
        meta_fpath.unlink()

    async def delete(self, id):
        fpath = self.id2path(id)
        if not fpath.exists():
            raise IndexError

        with fpath.open("rb") as f:
            msg = Message.from_json(f.read().decode('utf-8'))

        data_to_return = {'id': id, 'state': await self.get_message_state(id), 'message': msg}
        fpath.unlink()
        await self._delete_meta_file(id=id)
        return data_to_return
