#!/usr/bin/env python

import asyncio
import logging
import os
import re

from pathlib import Path

import asyncssh

from pypeman import channels, nodes, message, persistence

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger("asyncssh").setLevel(logging.WARNING)  # asyncssh's logs are really verbose

SFTP_TIMEOUT = 10  # timeout in seconds


class SFTPConnection():
    """
    FTP connection manager.
    """
    def __init__(self, host, port=22, credentials=None, hostkey=None):
        """
        :param host: SFTP host.
        :param port: SFTP port
        :param credentials: A tuple with (login, password)
        :param hostkey: Key to login with (optional if auth with login/pwd only)
        :return:
        """
        self.conn_args = {
            "host": host,
            "port": port,
            "connect_timeout": SFTP_TIMEOUT,
            "login_timeout": SFTP_TIMEOUT,
            "known_hosts": None,  # TODO: change that
        }

        if not (credentials or hostkey):
            raise TypeError("must have credentials or hostkey param")
        if credentials:
            self.conn_args["username"] = credentials[0]
            self.conn_args["password"] = credentials[1]
        if hostkey:
            self.conn_args["client_keys"] = [hostkey]

    async def __aenter__(self):
        self.client = await asyncssh.connect(
            **self.conn_args
        )
        self.sftp = await self.client.start_sftp_client()
        return self.sftp

    async def __aexit__(self, exc_type, exc_value, tb):
        self.sftp.exit()
        self.client.close()


class SFTPHelper():
    """
    SFTP helper to abstract sftp access.
    """

    def __init__(self, host, port=22, credentials=None, hostkey=None):
        self.host = host
        self.port = port
        self.hostkey = hostkey
        self.credentials = credentials

    async def list_dir(self, path):
        async with SFTPConnection(host=self.host, port=self.port,
                                  credentials=self.credentials,
                                  hostkey=self.hostkey) as sftp_conn:
            listdir = await sftp_conn.readdir(path)
            # TODO: test sort
            sortedlistdir = sorted(listdir, key=lambda sshfile: sshfile.attrs.mtime)
            return sortedlistdir

    async def download_file(self, filepath, encoding="utf-8"):
        """
        Download a file from sftp asynchronously.
        :param filepath: file path to download.
        :return: content of the file.
        """
        # Get file content from sftp
        logger.debug("SFTP download file from fpath %s", filepath)
        async with SFTPConnection(host=self.host, port=self.port,
                                  credentials=self.credentials,
                                  hostkey=self.hostkey) as sftp_conn:
            async with sftp_conn.open(filepath, asyncssh.FXF_READ, encoding=encoding) as fin:
                content = await fin.read()
        return content

    async def upload_file(self, filepath, content, encoding="utf-8"):
        """
        Upload a file to sftp.
        :param filepath: Path of file to create.
        :param content: Content to upload.
        """
        # write file in sftp
        logger.debug("SFTP upload file to fpath %s", filepath)
        async with SFTPConnection(host=self.host, port=self.port,
                                  credentials=self.credentials,
                                  hostkey=self.hostkey) as sftp_conn:
            async with sftp_conn.open(filepath, asyncssh.FXF_WRITE, encoding=encoding) as fout:
                content = await fout.write(content)
        return content

    async def rename(self, fromfilepath, tofilepath):
        """
        Rename a file from path to another path in ftp.
        :param fromfilepath: original file to rename.
        :param tofilepath: destination file.
        """
        logger.debug("SFTP rename file %s to %s", fromfilepath, tofilepath)
        async with SFTPConnection(host=self.host, port=self.port,
                                  credentials=self.credentials,
                                  hostkey=self.hostkey) as sftp_conn:
            await sftp_conn.rename(fromfilepath, tofilepath)

    async def delete(self, filepath):
        """
        Delete an SFTP file.
        :param filepath: File to delete.
        """
        logger.debug("SFTP Delete file %s", filepath)
        async with SFTPConnection(host=self.host, port=self.port,
                                  credentials=self.credentials,
                                  hostkey=self.hostkey) as sftp_conn:
            await sftp_conn.remove(filepath)

    async def file_exists(self, filepath):
        """
        Return True if a file exists in the SFTP, False otherwise
        :param filepath: File to check existence.
        """
        async with SFTPConnection(host=self.host, port=self.port,
                                  credentials=self.credentials,
                                  hostkey=self.hostkey) as sftp_conn:
            if await sftp_conn.exists(filepath):
                return True
            else:
                return False


class SFTPWatcherChannel(channels.BaseChannel):
    """
    Channel that watch sftp for file creation.
    """

    PERSISTENCE_TABLENAME = "sftpwatcher"

    def __init__(self, *args, host, port=22, credentials=None, hostkey=None, basedir="",
                 regex='.*', interval=6, delete_after=False, encoding="utf-8",
                 real_extensions=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.basedir = basedir
        self.interval = interval
        self.delete_after = delete_after
        self.re = re.compile(regex)
        self.encoding = encoding  # If set to None, read as bytes

        self.LAST_READ_MTIME_FIELDNAME = f"{self.name}_last_read_mtime"
        self.real_extensions = real_extensions  # list of extensions for exemple: [".csv", ".CSV"]

        self.sftphelper = SFTPHelper(
            host=host, port=port, credentials=credentials, hostkey=hostkey)
        self.backend = None
        self.last_read_mtime = 0

    async def get_last_read_mtime(self):
        """
        Get last read mtime in the persistence backend
        !caution! the channel must be started
        """
        last_read_mtime = await self.backend.get(
            self.PERSISTENCE_TABLENAME, self.LAST_READ_MTIME_FIELDNAME, default=0)
        return last_read_mtime

    async def set_last_read_mtime(self, mtime_value):
        """
        Set last read mtime in the persistence backend
        !caution! the channel must be started
        """
        await self.backend.store(
            self.PERSISTENCE_TABLENAME, self.LAST_READ_MTIME_FIELDNAME, mtime_value)
        self.last_read_mtime = mtime_value
        return mtime_value

    async def start(self):
        self.backend = await persistence.get_backend(loop=self.loop)
        self.last_read_mtime = await self.get_last_read_mtime()
        logger.debug("last_read_mtime at start is %s", str(self.last_read_mtime))
        await super().start()
        asyncio.create_task(self.watch_for_file())

    async def download_file(self, filename):
        """
        Download a file from sftp asynchronously.

        :param filepath: file path to download.

        :return: Content of the downloaded file.
        """
        if not self.is_stopped():
            logger.debug("file %s found in %s", filename, self.basedir)
            return await self.sftphelper.download_file(
                self.basedir + '/' + filename, encoding=self.encoding)

    async def get_file_and_process(self, filename):
        """
        Download a file from sftp and launch channel processing on msg with result as payload.
        Also add a `filepath` header with sftp relative path of downloaded file.

        :param filename: file to download relative to `basedir`.

        :return: processed result
        """
        logger.debug("start handling of %s", filename)
        if self.real_extensions:
            fpath = Path(self.basedir) / filename
            for extension in self.real_extensions:
                real_fpath = fpath.with_suffix(extension)
                if await self.sftphelper.file_exists(str(real_fpath)):
                    filename = real_fpath.name
                    break
            else:
                # If no related files
                logger.error(
                    "No %r related file to %s",
                    self.real_extensions, str(fpath))
        payload = await self.download_file(filename)

        msg = message.Message()
        msg.payload = payload
        msg.meta['filepath'] = self.basedir + '/' + filename

        if not self.is_stopped():
            await super().handle(msg)

    async def tick(self):
        """
        One iteration of watching.
        """
        sftp_ls = await self.sftphelper.list_dir(self.basedir)
        for filestat in sftp_ls:
            fname = filestat.filename
            if self.re.match(fname):
                file_mtime = filestat.attrs.mtime
                if self.last_read_mtime < file_mtime:
                    try:
                        # TODO: ask if a try/finally here is a good idea
                        await self.get_file_and_process(fname)
                    finally:
                        await self.set_last_read_mtime(file_mtime)

    async def watch_for_file(self):
        """
        Watch recursively for ftp new files.
        If file match regex, it is downloaded then processed in a message.
        """
        while not self.is_stopped():
            await asyncio.sleep(self.interval)
            try:
                await self.tick()
            except Exception as exc:
                logger.exception(exc)


class SFTPFileReader(nodes.BaseNode):
    """
    Node to read a file from FTP.
    """
    def __init__(self, host="", port=22, credentials=None, hostkey=None, filepath=None,
                 encoding="utf-8", **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.encoding = encoding  # If set to None, read as bytes
        self.sftphelper = SFTPHelper(host, port, credentials, hostkey)

    async def process(self, msg):

        filepath = nodes.choose_first_not_none(
            nodes.callable_or_value(self.filepath, msg),
            msg.meta.get('filepath'))
        content = await self.sftphelper.download_file(filepath, encoding=self.encoding)

        msg.payload = content
        msg.meta['filepath'] = filepath

        return msg


class SFTPFileDeleter(nodes.BaseNode):
    """
    Node to delete a file from SFTP.
    """
    def __init__(self, host="", port=22, credentials=None, hostkey=None, filepath=None,
                 extensions_to_rm=None, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.sftphelper = SFTPHelper(host, port, credentials, hostkey)
        self.extensions_to_rm = extensions_to_rm

    async def process(self, msg):
        filepath = nodes.choose_first_not_none(
            nodes.callable_or_value(self.filepath, msg),
            msg.meta.get('filepath'))
        base_fpath = os.path.splitext(msg.meta["filepath"])[0]
        try:
            await self.sftphelper.delete(filepath)
        except Exception:
            logger.exception("Trying to delete file %s but doesn't exists", filepath)
        if self.extensions_to_rm:
            for extension in self.extensions_to_rm:
                if await self.sftphelper.file_exists(f"{base_fpath}{extension}"):
                    await self.sftphelper.delete(f"{base_fpath}{extension}")
                    logger.debug("Meta file %s deleted", f"{base_fpath}{extension}")
        return msg


class SFTPFileWriter(nodes.BaseNode):
    """
    Node to write content to SFTP. File is first written with `.part` concatenated
    to its name then renamed to avoid partial upload.
    """
    def __init__(self, host, port=22, credentials=None, hostkey=None, filepath=None,
                 create_valid_file=False, validation_extension=".ok", encoding="utf-8",
                 **kwargs):

        super().__init__(**kwargs)

        self.filepath = filepath
        self.create_valid_file = create_valid_file
        self.validation_extension = validation_extension
        self.sftphelper = SFTPHelper(host, port, credentials, hostkey)
        self.encoding = encoding

    async def process(self, msg):

        filepath = nodes.choose_first_not_none(
            nodes.callable_or_value(self.filepath, msg),
            msg.meta.get('filepath'))
        content = msg.payload
        if isinstance(content, str):
            content = content.encode(self.encoding)

        await self.sftphelper.upload_file(filepath + '.part', content)
        await self.sftphelper.rename(filepath + '.part', filepath)
        if self.create_valid_file:
            validation_path = Path(filepath).with_suffix(self.validation_extension)
            await self.sftphelper.upload_file(str(validation_path), b"")

        return msg
