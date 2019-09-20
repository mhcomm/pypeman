import asyncio
import sys

# code copieds from https://github.com/feenes/mytb v0.0.13
#  file: mytb/aio/compat.py

# Not tested for pre 3.4 versions
assert sys.version_info >= (3, 4)

patched = []  # keeps track of what has been patched


def patch():
    """
    patches pre 3.7 asyncios with some hacky but 'compatible' back ports
    """

    if not hasattr(asyncio, "run"):
        # asyncio.run function has been added to asyncio in
        # Python 3.7 on a provisional basis.
        #

        def run(coro):
            loop = asyncio.get_event_loop()
            loop.create_task(coro)
            loop.run_forever()

        asyncio.run = run
        patched.append("asyncio.run")

    if not hasattr(asyncio, "all_tasks"):
        # from python 3.7 on asyncio.all_tasks replaces asyncio.Task.all_tasks,
        # which will be removed in Python 3.9
        asyncio.all_tasks = asyncio.Task.all_tasks
        patched.append("asyncio.all_tasks")
