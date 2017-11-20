import datetime

from pypeman import channels, message

from aiocron import crontab


class CronChannel(channels.BaseChannel):
    """
    This channel Launch processing at specified time interval.
    The first node of the channel receive a payload with the
    datetime of execution.

    :params cron: This set the interval. Accept any
        `aiocron <https://github.com/gawel/aiocron/>`_ compatible string.

    """

    def __init__(self, *args, cron='', **kwargs):
        super().__init__(*args, **kwargs)
        self.cron = cron

    async def start(self):
        await super().start()
        crontab(self.cron, func=self.tic, start=True, loop=self.loop)

    async def tic(self):
        msg = message.Message()
        msg.payload = datetime.datetime.now()
        await self.handle(msg)
