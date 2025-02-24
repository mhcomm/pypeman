import tempfile
from pathlib import Path

TEMPDIR = tempfile.TemporaryDirectory()
RETRY_STORE_PATH = Path(TEMPDIR.name)
