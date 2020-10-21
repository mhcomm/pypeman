"""
Global graph related pypeman functions.

On the long run some commands of pypeman.commands might be candidates
to be moved into this module
"""


import time

from pypeman import channels
from pypeman.errors import PypemanError


def wait_for_loop(tmax=5.0):
    """
    wait until the loop variable of a pypeman graph
    has been initialized

    can be used from any thread that's not the main thread
    """
    # TODO: might factor out this function to a helper module
    loop = None
    steps = int(tmax / 0.1)
    for i in range(steps, -1, -1):
        try:
            channel = channels.all_channels[0]
            # print("channel =", channel)
            loop = channel.loop
            break
        except Exception:
            if i == 0:
                raise PypemanError("couldn't obtain graph's loop")
        time.sleep(0.1)
    return loop
