import asyncio


class Sleeper:
    """
    Group of sleep calls allowing instant cancellation of all

    found at: https://stackoverflow.com/questions/37209864/interrupt-all-asyncio-sleep-currently-executing
    """

    def __init__(self, loop):
        self.loop = loop
        self.tasks = set()

    async def sleep(self, delay, result=None):
        """
        a sleep function, that can be interrupted
        """
        coro = asyncio.sleep(delay, result=result)
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            return result
        finally:
            self.tasks.remove(task)

    def cancel_all_helper(self):
        """
        Cancel all pending sleep tasks
        """
        cancelled = set()
        for task in self.tasks:
            if task.cancel():
                cancelled.add(task)
        return cancelled

    async def cancel_all(self):
        """
        Coroutine cancelling tasks
        """
        cancelled = self.cancel_all_helper()
        if self.tasks:
            await asyncio.wait(self.tasks)
        self.tasks -= cancelled
        return len(cancelled)
