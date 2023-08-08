from aiohttp import web

from pypeman.plugins.base import BasePlugin
from pypeman.plugins.remoteadmin import urls


class RemoteAdminPlugin(BasePlugin):

    def __init__(self, host="127.0.0.1", port=8091, url_prefix=""):
        super().__init__()
        self.app = web.Application()
        self.host = host
        self.port = port
        self.url_prefix = url_prefix
        self.runner = web.AppRunner(self.app)

    def ready(self):
        urls.init_urls(self.app, prefix=self.url_prefix)

    async def start(self):
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def stop(self):
        await self.runner.cleanup()
