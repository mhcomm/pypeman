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

    async def start(self):
        await super().start()
        crontab(self.cron, func=self.tic, start=True)

    async def tic(self):
        msg = message.Message()
        msg.payload = datetime.datetime.now()
        await self.handle(msg)
