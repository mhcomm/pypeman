from pathlib import Path


class SFTPFileStat():
    """
        Try to ''transform'' a Pathlib.Path file Object in an asyncssh sftp file Object
    """
    def __init__(self, file):
        self.stat_result = file.stat()
        self.mtime = self.stat_result.st_mtime
        self.ctime = self.stat_result.st_ctime
        self.filename = file.name

    def __getattr__(self, attr):
        if attr == "attrs":
            return self
        if attr in self.__dict__:
            return self.__dict__[attr]
        if hasattr(self.stat_result, attr):
            return getattr(self.stat_result, attr)
        raise AttributeError(f"'SFTPFileStat' object has no attribute '{attr}'")


class MockedSFTPFile():
    def __init__(self, path, fmode, encoding=None):
        self.path = path
        self.fmode = fmode
        self.encoding = encoding

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, tb):
        pass

    async def read(self):
        with Path(self.path).open(self.fmode, encoding=self.encoding) as fin:
            f_content = fin.read()
        return f_content


class MockedConn():
    async def readdir(self, path):
        files = []
        for fpath in Path(path).iterdir():
            files.append(SFTPFileStat(fpath))
        return files

    def open(self, path, fmode, encoding=None):
        return MockedSFTPFile(path=path, fmode=fmode, encoding=encoding)

    async def exists(self, path):
        return Path(path).exists()


class MockedSFTPConnection():
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return MockedConn()

    async def __aexit__(self, exc_type, exc_value, tb):
        pass
