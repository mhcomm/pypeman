from aiohttp import web

from pypeman.plugins.base import BasePlugin
from pypeman.plugins.remoteadmin import urls


class WSRemoteAdminPlugin(BasePlugin):

    def __init__(self, host="127.0.0.1", port=8000):
        super().__init__()
        self.app = web.Application()
        self.host = host
        self.port = port
        self.runner = web.AppRunner(self.app)

    def ready(self):
        urls.init_urls(self.app)

    async def start(self):
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def stop(self):
        await self.runner.cleanup()
