import asyncio
from ftplib import FTP
import re
from io import StringIO, BytesIO
import pickle

# For compatibility
from asyncio import async as ensure_future

from pypeman import channels, nodes, message

from concurrent.futures import ThreadPoolExecutor

# Can be redefined
default_thread_pool = ThreadPoolExecutor(max_workers=3)

class FTPConnection():
    """
    FTP connection manager.
    """
    def __init__(self, host, port, credentials):
        """
        :param host: FTP host.
        :param port: FTP port
        :param credentials: A tuple with (login, password,)
        :return:
        """
        self.host = host
        self.port = port
        self.credentials = credentials

    def __enter__(self):
        self.ftp = FTP(self.host)
        self.ftp.login(*self.credentials)
        return self.ftp

    def __exit__(self, type, value, tb):
        try:
            self.ftp.quit()
        except:
            self.ftp.close()


class FTPHelper():
    """
    FTP helper to abstract ftp access.
    """
    # TODO reuse ftp connection if possible

    def __init__(self, host, port, credentials):
        self.host = host
        self.port = port
        self.credentials = credentials

    def list_dir(self, path):
        with FTPConnection(self.host, self.port, self.credentials) as ftp_conn:
            ftp_conn.cwd(path)
            return set(ftp_conn.nlst())

    def download_file(self, filepath):
        """
        Download a file from ftp asynchronously.
        :param filepath: file path to download.
        :return: content of the file.
        """
        output = BytesIO()

        # Get file from ftp
        with FTPConnection(self.host, self.port, self.credentials) as ftp_conn:
            ftp_conn.retrbinary("RETR %s" % filepath, output.write)

        content = output.getvalue()
        output.close()

        return content

    def upload_file(self, filepath, content):
        """
        Upload an file to ftp.
        :param filepath: Path of file to create.
        :param content: Content to upload.
        """
        input = BytesIO(content)

        # Get file from ftp
        with FTPConnection(self.host, self.port, self.credentials) as ftp_conn:
            ftp_conn.storbinary('STOR %s' % filepath, input)

        input.close()

        return content

    def rename(self, fromfilepath, tofilepath):
        """
        Rename a file from path to another path in ftp.
        :param fromfilepath: original file to rename.
        :param tofilepath: destination file.
        """
        with FTPConnection(self.host, self.port, self.credentials) as ftp_conn:
            ftp_conn.rename(fromfilepath, tofilepath)

    def delete(self, filepath):
        """
        Delete an FTP file.
        :param filepath: File to delete.
        """
        with FTPConnection(self.host, self.port, self.credentials) as ftp_conn:
            ftp_conn.delete(filepath)


class FTPWatcherChannel(channels.BaseChannel):
    """
    Channel that watch ftp for file creation.
    """
    def __init__(self, *args, host="", port=21, credentials="", basedir="", regex='.*',
                 interval=60, delete_after=False, encoding="utf-8",
                 thread_pool=None, sort_function=sorted, **kwargs):
        super().__init__(*args, **kwargs)

        self.basedir = basedir
        self.interval = interval
        self.delete_after = delete_after
        self.re = re.compile(regex)
        self.encoding = encoding
        self.sort_function = sort_function
        self.ls_prev = set()

        # pool used to make ftp connection out of thread
        self.executor = thread_pool or default_thread_pool

        self.ftphelper = FTPHelper(host, port, credentials)

    @asyncio.coroutine
    def start(self):
        yield from super().start()
        ensure_future(self.watch_for_file(), loop=self.loop)

    def download_file(self, filename):
        """
        Download a file from ftp asynchronously.

        :param filepath: file path to download.

        :return: Content of the downloaded file.
        """
        if not self.is_stopped():
            return self.ftphelper.download_file(self.basedir + '/' + filename)

    @asyncio.coroutine
    def get_file_and_process(self, filename):
        """
        Download a file from ftp and launch channel processing on msg with result as payload.
        Also add a `filepath` header with ftp relative path of downloaded file.

        :param filename: file to download relative to `basedir`.

        :return: processed result
        """

        payload = yield from self.loop.run_in_executor(self.executor, self.download_file, filename)

        msg = message.Message()
        msg.payload = payload
        msg.meta['filepath'] = self.basedir + '/' + filename

        if not self.is_stopped():
            result = yield from super().handle(msg)

            if self.delete_after:
                yield from self.loop.run_in_executor(
                                self.executor,
                                self.ftphelper.delete,
                                self.basedir + '/' + filename)

            return result

    @asyncio.coroutine
    def tick(self):
        """
        One iteration of watching.
        """

        ls = yield from self.loop.run_in_executor(self.executor, self.ftphelper.list_dir, self.basedir)

        # Make diff from previous one.
        added = self.sort_function(ls-self.ls_prev)
        self.ls_prev = ls

        for filename in added:
            if self.re.match(filename) and not self.is_stopped():
                ensure_future(self.get_file_and_process(filename), loop=self.loop)

    @asyncio.coroutine
    def watch_for_file(self):
        """
        Watch recursively for ftp new files.
        If file match regex, it is downloaded then processed in a message.
        """
        yield from asyncio.sleep(self.interval, loop=self.loop)
        try:
            yield from self.tick()
        finally:
            if not self.is_stopped():
                ensure_future(self.watch_for_file(), loop=self.loop)


class FTPFileReader(nodes.ThreadNode):
    """
    Node to read a file from FTP.
    """
    def __init__(self, host="", port=21, credentials=None, filepath=None, **kwargs):

        super().__init__(**kwargs)

        self.filepath = filepath

        self.ftphelper = FTPHelper(host, port, credentials)

    def process(self, msg):

        filepath = nodes.choose_first_not_none(nodes.callable_or_value(self.filepath, msg), msg.meta.get('filepath'))

        content = self.ftphelper.download_file(filepath)

        msg.payload = content
        msg.meta['filepath'] = filepath

        return msg


class FTPFileDeleter(nodes.ThreadNode):
    """
    Node to delete a file from FTP.
    """
    def __init__(self, host="", port=21, credentials=None, filepath=None, **kwargs):

        super().__init__(**kwargs)

        self.filepath = filepath

        self.ftphelper = FTPHelper(host, port, credentials)

    def process(self, msg):

        filepath = nodes.choose_first_not_none(nodes.callable_or_value(self.filepath, msg), msg.meta.get('filepath'))

        self.ftphelper.delete(filepath)

        return msg

class FTPFileWriter(nodes.ThreadNode):
    """
    Node to write content to FTP. File is first written with `.part` concatenated
    to its name then renamed to avoid partial upload.
    """
    def __init__(self, host="", port=21, credentials=None, filepath=None, **kwargs):

        super().__init__(**kwargs)

        self.filepath = filepath

        self.ftphelper = FTPHelper(host, port, credentials)

    def process(self, msg):

        filepath = nodes.choose_first_not_none(nodes.callable_or_value(self.filepath, msg), msg.meta.get('filepath'))

        self.ftphelper.upload_file(filepath + '.part', msg.payload)
        self.ftphelper.rename(filepath + '.part', filepath)

        return msg
