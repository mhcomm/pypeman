import os
import sys
import threading
import time

from contextlib import contextmanager

import pypeman.commands

from pypeman.graph import wait_for_loop


tst_lock = threading.Lock()


# migth refactor to a helper directory
@contextmanager
def chdir(path=None):
    """
        enter a certain directory for a given test
    """
    if path is None:
        path = os.path.realpath(os.path.dirname(__file__))
    prev_dir = os.getcwd()
    os.chdir(path)
    yield prev_dir
    os.chdir(prev_dir)


def start_pm_graph():
    try:
        sys.argv[1:] = ["--no-daemon"]
        pypeman.commands.start()
    except SystemExit:
        pass


def tst_thread(errors):
    """
    testing thread for test_one
    """
    loop = wait_for_loop()
    print("got loop", loop)
    time.sleep(4)
    # with tst_lock:
    #     errors.append("bad")
    loop.stop()


def test_one():
    print("\n\nNAME:", __name__, "\n")
    with chdir():
        errors = []
        thrd = threading.Thread(
            target=tst_thread,
            kwargs=dict(errors=errors),
            )
        thrd.start()
        start_pm_graph()
        print("END OF GRAPH")
        for error in errors:
            print(error)
        assert not errors, "There should have been 0 errors"
