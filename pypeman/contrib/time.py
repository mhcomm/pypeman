import asyncio
import datetime

from pypeman import channels, message

from aiocron import crontab

class CronChannel(channels.BaseChannel):
    """
    Periodic execution of tasks.
    Launch processing at specified time interval.
    """

    def __init__(self, *args, cron='', **kwargs):
        super().__init__(*args, **kwargs)
        self.cron = cron

    @asyncio.coroutine
    def start(self):
        super().start()
        crontab(self.cron, func=self.tic, start=True)

    @asyncio.coroutine
    def tic(self):
        msg = message.Message()
        msg.payload = datetime.datetime.now()
        yield from self.handle(msg)
