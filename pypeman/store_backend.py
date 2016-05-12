#!/usr/bin/env python


class NullStoreBackend():
    """ For testing purpose """
    def store(self, message):
        pass


class FileStoreBackend():
    def __init__(self, path, filename, channel):
        self.path = path
        self.filename = filename
        self.counter = 0
        self.channel = channel

    def store(self, message):
        today = datetime.now()

        context = {'counter':self.counter,
                   'year': today.year,
                   'month': today.month,
                   'day': today.day,
                   'hour': today.hour,
                   'second': today.second,
                   'muid': message.uuid,
                   'cuid': getattr(self.channel, 'uuid', '???')
                   }

        filepath = os.path.join(self.path, self.filename % context)

        try:
            # Make missing dir if any
            os.makedirs(os.path.dirname(filepath))
        except FileExistsError:
            pass

        with open(filepath, 'w') as file_:
            file_.write(message.payload)

        self.counter += 1


class SQLiteStoreBackend():
    dependencies = ['sqlite3']
    def __init__(self, path):
        self.path = path
        self.counter = 0
        conn = sqlite3.connect(path)
        self.cursor = conn.cursor()
    def store(self, message):
        pass
