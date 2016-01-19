import os
import tempfile

def mktempfname(suffix='', prefix='tmp', dir=None):
    """ creates a temporary file name 
        in order to be nice to windows the file handle is closed immediately
    """
    fdesc, fname = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
    os.close(fdesc)
    return fname


