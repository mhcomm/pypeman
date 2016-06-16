import logging
from pypeman.message import Message
import sqlite3
import json
import os
import re

logger = logging.getLogger("pypeman.store")

DATE_FORMAT = '%Y%m%d_%H%M'


MESSAGE_STATE = {

}


class MessageStore():


    def store(self, msg):
        """
        Store a message in the store.
        :param msg: The message to store.
        :return: Id for this specific message.
        """
        pass

    def change_message_state(self, id, new_state):
        """
        Change the `id` message state.
        :param id: Message specific store id.
        :param new_state: Target state.
        :return: -
        """
        pass

    def get(self, id):
        """
        Return one message corresponding to given `id` with his status.
        :param id: Message id. Message store dependant.
        :return: A dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """
        pass

    def search(self, order_by='timestamp'):
        """
        Return a list of message with store specific `id` and processed status.
        :param start: First element.
        :param count: Count of elements since first element.
        :param order_by: Message order. Allowed values : ['timestamp', 'status'].
        :return: A list of dict `{'id':<message_id>, 'state': <message_state>, 'message': <message_object>}`.
        """
        pass


class NoneMessageStore(MessageStore):
    """ For testing purpose """

    def store(self, msg):
        return ''

    def get(self, id):
        raise NotImplementedError

    def search(self, order_by='timestamp'):
        raise NotImplementedError


class NullMessageStore(MessageStore):
    """ For testing purpose """

    def store(self, msg):
        logger.debug("Should store message %s", msg)
        return 'fake_id'

    def get(self, id):
        return {'id':id, 'state': 'processed', 'message': None}

    def search(self, order_by='timestamp'):
        return []


class FileMessageStore(MessageStore):
    def __init__(self, path=None):
        super().__init__()
        self.base_path = path

        # Match msg file name
        self.msg_re = re.compile(r'^([0-9]{8})_([0-9]{2})([0-9]{2})_[0-9abcdef]*$')

    def store(self, msg):
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
        with open(os.path.join(self.base_path, file_path), "w") as f:
            f.write(msg.to_json())

        self.change_message_state(file_path, Message.PENDING)

        return file_path

    def change_message_state(self, id, new_state):
        with open(os.path.join(self.base_path, id + '.state'), "w") as f:
            f.write(new_state)

    def get_message_state(self, id):
        with open(os.path.join(self.base_path, id + '.state'), "r") as f:
            state = f.read()
            return state

    def get(self, id):
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

        for year in self.sorted_list_directories(self.base_path, reverse=reverse):
            for month in self.sorted_list_directories(os.path.join(self.base_path, year), reverse=reverse):
                for day in self.sorted_list_directories(os.path.join(self.base_path, year, month), reverse=reverse):
                    for msg_name in sorted(os.listdir(os.path.join(self.base_path, year, month, day)), reverse=reverse):
                        found = self.msg_re.match(msg_name)
                        if found:
                            id = os.path.join(year, month, day, msg_name)
                            yield self.get(id)
