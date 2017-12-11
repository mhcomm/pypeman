import os
import re
import asyncio
import logging
from collections import OrderedDict

from pypeman.message import Message

from pypeman.errors import PypemanConfigError

logger = logging.getLogger("pypeman.store")

DATE_FORMAT = '%Y%m%d_%H%M'


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

    async def change_message_state(self, id, new_state):
        """
        Change the `id` message state.

        :param id: Message specific store id.
        :param new_state: Target state.
        """

    async def get(self, id):
        """
        Return one message corresponding to given `id` with his status.

        :param id: Message id. Message store dependant.
        :return: A dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """

    async def search(self, start=0, count=10, order_by='timestamp'):
        """
        Return a list of message with store specific `id` and processed status.

        :param start: First element.
        :param count: Count of elements since first element.
        :param order_by: Message order. Allowed values : ['timestamp', 'status'].
        :return: A list of dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """

    async def total(self):
        """
        :return: total count of messages
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

    async def search(self, **kwargs):
        return None

    async def total(self):
        return 0


class FakeMessageStoreFactory(MessageStoreFactory):
    """ Return an Fake message store """
    def get_store(self, store_id):
        return FakeMessageStore()


class FakeMessageStore(MessageStore):
    """ For testing purpose """

    async def store(self, msg):
        logger.debug("Should store message %s", msg)
        return 'fake_id'

    async def get(self, id):
        return {'id':id, 'state': 'processed', 'message': None}

    async def search(self, **kwargs):
        return []

    async def total(self):
        return 0


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
        self.messages[msg_id] = {'id': msg_id, 'state': Message.PENDING, 'timestamp': msg.timestamp, 'message': msg.to_dict()}
        return msg_id

    async def change_message_state(self, id, new_state):
        self.messages[id]['state'] = new_state

    async def get(self, id):
        resp = dict(self.messages[id])
        resp['message'] = Message.from_dict(resp['message'])
        return resp

    async def search(self, start=0, count=10, order_by='timestamp'):

        if order_by.startswith('-'):
            reverse = True
            sort_key = order_by[1:]
        else:
            reverse = False
            sort_key = order_by

        result = []
        for value in sorted(self.messages.values(), key=lambda x: x[sort_key], reverse=reverse):

            resp = dict(value)
            resp['message'] = Message.from_dict(resp['message'])

            result.append(resp)

        return result[start: start + count]

    async def total(self):
        return len(self.messages)

class FileMessageStoreFactory(MessageStoreFactory):
    """
    Generate a FileMessageStore message store instance.
    Store a file in `<base_path>/<store_id>/<month>/<day>/` hierachy.
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
    """ Store a file in `<base_path>/<store_id>/<month>/<day>/` hierachy."""
    # TODO file access should be done in another thread. Waiting for file backend.

    def __init__(self, path, store_id):
        super().__init__()

        self.base_path = os.path.join(path, store_id)

        # Match msg file name
        self.msg_re = re.compile(r'^([0-9]{8})_([0-9]{2})([0-9]{2})_[0-9abcdef]*$')

        try:
            # Try to make dirs if necessary
            os.makedirs(os.path.join(self.base_path))
        except FileExistsError:
            pass

        self._total = 0

    async def start(self):
        self._total = await self.count_msgs()

    async def store(self, msg):
        """ Store a file in `<base_path>/<store_id>/<month>/<day>/` hierachy."""
        # TODO implement a safer store to avoid broken messages

        # The filename is the file id
        filename = "{}_{}".format(msg.timestamp.strftime(DATE_FORMAT), msg.uuid)
        dirs = os.path.join(str(msg.timestamp.year), "%02d" % msg.timestamp.month, "%02d" % msg.timestamp.day)

        try:
            # Try to make dirs if necessary
            os.makedirs(os.path.join(self.base_path, dirs))
        except FileExistsError:
            pass

        file_path = os.path.join(dirs, filename)

        # Write message to file
        with open(os.path.join(self.base_path, file_path), "w") as f:
            f.write(msg.to_json())

        await self.change_message_state(file_path, Message.PENDING)

        self._total += 1

        return file_path

    async def change_message_state(self, id, new_state):
        with open(os.path.join(self.base_path, id + '.meta'), "w") as f:
            f.write(new_state)

    async def get_message_state(self, id):
        with open(os.path.join(self.base_path, id + '.meta'), "r") as f:
            state = f.read()
            return state

    async def get(self, id):
        if not os.path.exists(os.path.join(self.base_path, id)):
            raise IndexError

        with open(os.path.join(self.base_path, id), "rb") as f:
            msg = Message.from_json(f.read().decode('utf-8'))
            return {'id': id, 'state': await self.get_message_state(id), 'message': msg}

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
                    for msg_name in sorted(os.listdir(os.path.join(self.base_path, year, month, day))):
                        found = self.msg_re.match(msg_name)
                        if found:
                            count +=1
        return count

    async def search(self, start=0, count=10, order_by='timestamp'):
        # TODO better performance for slicing by counting file in dirs ?
        if order_by.startswith('-'):
            reverse = True
            sort_key = order_by[1:]
        else:
            reverse = False
            sort_key = order_by

        # TODO handle sort_key

        result = []
        end = start + count

        position = 0
        for year in await self.sorted_list_directories(os.path.join(self.base_path), reverse=reverse):
            for month in await self.sorted_list_directories(os.path.join(self.base_path, year), reverse=reverse):
                for day in await self.sorted_list_directories(os.path.join(self.base_path, year, month), reverse=reverse):
                    for msg_name in sorted(os.listdir(os.path.join(self.base_path, year, month, day)), reverse=reverse):
                        found = self.msg_re.match(msg_name)
                        if found:
                            if start <= position < end:
                                mid = os.path.join(year, month, day, msg_name)
                                result.append(await self.get(mid))
                            position += 1

        return result

    async def total(self):
        return self._total
