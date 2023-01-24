import asyncio
import logging
import re

from ftplib import FTP
from io import BytesIO
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor

from pypeman import channels, nodes, message

# Can be redefined
default_thread_pool = ThreadPoolExecutor(max_workers=3)

logger = logging.getLogger(__name__)


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
        except Exception:
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
                 thread_pool=None, sort_function=sorted, real_extensions=None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.basedir = basedir
        self.interval = interval
        self.delete_after = delete_after
        self.re = re.compile(regex)
        self.encoding = encoding
        self.sort_function = sort_function
        self.real_extensions = real_extensions  # list of extensions for exemple: [".csv", ".CSV"]
        self.ls_prev = set()

        # pool used to make ftp connection out of thread
        self.executor = thread_pool or default_thread_pool

        self.ftphelper = FTPHelper(host, port, credentials)

    async def start(self):
        await super().start()
        asyncio.create_task(self.watch_for_file())

    def download_file(self, filename):
        """
        Download a file from ftp asynchronously.

        :param filepath: file path to download.

        :return: Content of the downloaded file.
        """
        if not self.is_stopped():
            return self.ftphelper.download_file(self.basedir + '/' + filename)

    async def get_file_and_process(self, filename):
        """
        Download a file from ftp and launch channel processing on msg with result as payload.
        Also add a `filepath` header with ftp relative path of downloaded file.

        :param filename: file to download relative to `basedir`.

        :return: processed result
        """

        payload = await self.loop.run_in_executor(self.executor, self.download_file, filename)

        msg = message.Message()
        msg.payload = payload
        msg.meta['filepath'] = self.basedir + '/' + filename

        if not self.is_stopped():
            result = await super().handle(msg)

            if self.delete_after:
                await self.loop.run_in_executor(
                                self.executor,
                                self.ftphelper.delete,
                                self.basedir + '/' + filename)

            return result

    async def tick(self):
        """
        One iteration of watching.
        """

        ls = await self.loop.run_in_executor(self.executor, self.ftphelper.list_dir, self.basedir)

        # Make diff from previous one.
        added = self.sort_function(ls-self.ls_prev)
        self.ls_prev = ls

        for filename in added:
            if self.re.match(filename) and not self.is_stopped():
                if self.real_extensions:
                    for extension in self.real_extensions:
                        real_fname = str(Path(filename).with_suffix(extension))
                        if real_fname in ls:
                            filename = real_fname
                            break
                    else:
                        # If no related files
                        # TODO : ask if raise exc or not
                        logger.error(
                            "No %r related file to %s",
                            self.real_extensions, filename)
                        continue
                asyncio.create_task(self.get_file_and_process(filename))

    async def watch_for_file(self):
        """
        Watch recursively for ftp new files.
        If file match regex, it is downloaded then processed in a message.
        """
        await asyncio.sleep(self.interval)
        try:
            await self.tick()
        finally:
            if not self.is_stopped():
                asyncio.create_task(self.watch_for_file())


class FTPFileReader(nodes.ThreadNode):
    """
    Node to read a file from FTP.
    """
    def __init__(self, host="", port=21, credentials=None, filepath=None, **kwargs):

        super().__init__(**kwargs)

        self.filepath = filepath

        self.ftphelper = FTPHelper(host, port, credentials)

    def process(self, msg):

        filepath = nodes.choose_first_not_none(
            nodes.callable_or_value(self.filepath, msg),
            msg.meta.get('filepath'))

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

        filepath = nodes.choose_first_not_none(
            nodes.callable_or_value(self.filepath, msg),
            msg.meta.get('filepath'))

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

        filepath = nodes.choose_first_not_none(
            nodes.callable_or_value(self.filepath, msg),
            msg.meta.get('filepath'))

        self.ftphelper.upload_file(filepath + '.part', msg.payload)
        self.ftphelper.rename(filepath + '.part', filepath)

        return msg
