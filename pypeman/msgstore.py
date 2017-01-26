import logging
from pypeman.message import Message
import os
import re
from collections import OrderedDict

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

    def store(self, msg):
        """
        Store a message in the store.

        :param msg: The message to store.
        :return: Id for this specific message.
        """

    def change_message_state(self, id, new_state):
        """
        Change the `id` message state.

        :param id: Message specific store id.
        :param new_state: Target state.
        """

    def get(self, id):
        """
        Return one message corresponding to given `id` with his status.

        :param id: Message id. Message store dependant.
        :return: A dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """

    def search(self, order_by='timestamp'):
        """
        Return a list of message with store specific `id` and processed status.

        :param start: First element.
        :param count: Count of elements since first element.
        :param order_by: Message order. Allowed values : ['timestamp', 'status'].
        :return: A list of dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """


class NullMessageStoreFactory(MessageStoreFactory):
    """ Return an NullMessageStore that do nothing at all. """
    def get_store(self, store_id):
        return NullMessageStore()


class NullMessageStore(MessageStore):
    """ For testing purpose """

    def store(self, msg):
        return None

    def get(self, id):
        return None

    def search(self, order_by='timestamp'):
        return None


class FakeMessageStoreFactory(MessageStoreFactory):
    """ Return an Fake message store """
    def get_store(self, store_id):
        return FakeMessageStore()


class FakeMessageStore(MessageStore):
    """ For testing purpose """

    def store(self, msg):
        logger.debug("Should store message %s", msg)
        return 'fake_id'

    def get(self, id):
        return {'id':id, 'state': 'processed', 'message': None}

    def search(self, order_by='timestamp'):
        return []


class MemoryMessageStoreFactory(MessageStoreFactory):
    """ Return a Memory message store. All message are loose at pypeman stop. """
    def __init__(self):
        self.base_dict = {}

    def get_store(self, store_id):
        return MemoryMessageStore(self.base_dict, store_id)


class MemoryMessageStore(MessageStore):
    """ Store messages in memory """

    def __init__(self, base_dict, store_id):
        super().__init__()
        self.messages = base_dict.setdefault(store_id, OrderedDict())

    def store(self, msg):
        msg_id = msg.uuid.hex
        self.messages[msg_id] = {'id': msg_id, 'state': Message.PENDING, 'message': msg.to_dict()}
        return msg_id

    def change_message_state(self, id, new_state):
        self.messages[id]['state'] = new_state

    def get(self, id):
        resp = dict(self.messages[id])
        resp['message'] = Message.from_dict(resp['message'])
        return resp

    def search(self, order_by='timestamp'):

        for value in self.messages.values():

            resp = dict(value)
            resp['message'] = Message.from_dict(resp['message'])

            yield resp


class FileMessageStoreFactory(MessageStoreFactory):
    """
    Generate a FileMessageStore message store instance.
    Store a file in `<base_path>/<store_id>/<month>/<day>/` hierachy.
    """

    # TODO add an option to reguraly archive old file or delete them
    def __init__(self, path):
        super().__init__()
        self.base_path = path

    def get_store(self, store_id):
        return FileMessageStore(self.base_path, store_id)


class FileMessageStore(MessageStore):
    """ Store a file in `<base_path>/<store_id>/<month>/<day>/` hierachy."""

    def __init__(self, path, store_id):
        super().__init__()

        self.base_path = os.path.join(path, store_id)

        # Match msg file name
        self.msg_re = re.compile(r'^([0-9]{8})_([0-9]{2})([0-9]{2})_[0-9abcdef]*$')

    def store(self, msg):
        """ Store a file in `<base_path>/<store_id>/<month>/<day>/` hierachy."""

        # The filename is the file id
        filename = "{}_{}".format(msg.timestamp.strftime(DATE_FORMAT), msg.uuid.hex)
        dirs = os.path.join(str(msg.timestamp.year), "%02d" % msg.timestamp.month, "%02d" % msg.timestamp.day)

        try:
            # Try to make dirs if necessary
            os.makedirs(os.path.join(self.base_path, dirs))
        except FileExistsError:
            pass

        file_path = os.path.join(dirs, filename)

        # Write message to file
        # TODO use async file writing
        with open(os.path.join(self.base_path, file_path), "w") as f:
            f.write(msg.to_json())

        self.change_message_state(file_path, Message.PENDING)

        return file_path

    def change_message_state(self, id, new_state):
        with open(os.path.join(self.base_path, id + '.meta'), "w") as f:
            f.write(new_state)

    def get_message_state(self, id):
        with open(os.path.join(self.base_path, id + '.meta'), "r") as f:
            state = f.read()
            return state

    def get(self, id):
        if not os.path.exists(os.path.join(self.base_path, id)):
            raise IndexError

        with open(os.path.join(self.base_path, id), "rb") as f:
            msg = Message.from_json(f.read().decode('utf-8'))
            return {'id':id, 'state': self.get_message_state(id), 'message': msg}

    def sorted_list_directories(self, path, reverse=True):
        """
        :param path: Base path
        :param reverse: reverse order
        :return: List of directories in specified path ordered
        """
        return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))], reverse=reverse)

    def search(self, order_by='timestamp'):
        reverse = (order_by == 'timestamp')

        for year in self.sorted_list_directories(os.path.join(self.base_path), reverse=reverse):
            for month in self.sorted_list_directories(os.path.join(self.base_path, year), reverse=reverse):
                for day in self.sorted_list_directories(os.path.join(self.base_path, year, month), reverse=reverse):
                    for msg_name in sorted(os.listdir(os.path.join(self.base_path, year, month, day)), reverse=reverse):
                        found = self.msg_re.match(msg_name)
                        if found:
                            id = os.path.join(year, month, day, msg_name)
                            yield self.get(id)
