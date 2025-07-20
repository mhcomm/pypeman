"""See module- (package-? ie the __init__) level documentation."""

from argparse import ArgumentParser
from argparse import Namespace

from aiohttp import web

from . import urls
from ..base import BasePlugin
from ..base import CommandPluginMixin
from ..base import TaskPluginMixin
from ...conf import settings


class RemoteAdminPlugin(BasePlugin, CommandPluginMixin, TaskPluginMixin):
    """Provides the `shell` command, alongside the related web server."""

    def __init__(self):
        # conf = settings.get("REMOTE_ADMIN_WEBSOCKET_CONFIG") or settings.REMOTE_ADMIN_CONFIG
        conf = getattr(settings, "REMOTE_ADMIN_WEBSOCKET_CONFIG", settings.REMOTE_ADMIN_CONFIG)
        self.host = str(conf["host"])
        self.port = int(conf["port"])
        self.url_prefix = str(conf.get("url", ""))

        # old old:
        # remote = remoteadmin.RemoteAdminServer(loop=loop, **settings.REMOTE_ADMIN_WEBSOCKET_CONFIG)
        # webadmin = remoteadmin.WebAdmin(loop=loop, **settings.REMOTE_ADMIN_WEB_CONFIG)

        # old plugin:
        # plugins were all started with `cls()`
        # __init__(self, host: str="127.0.0.1", port=8091, url_prefix="")
        # and the `shell` command:
        # remoteadmin.PypemanShell(url='ws://%s:%s' % (settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['host'],
        #                          settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['port'])).cmdloop()

    @classmethod
    def command_name(cls):
        return "shell"

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument("host", nargs='?', help="override settings' host")
        parser.add_argument("port", nargs='?', help="override settings' port")

    async def command(self, options: Namespace):
        assert not "implemented"

    async def task_start(self):
        self.app = web.Application()
        urls.init_urls(self.app, prefix=self.url_prefix)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def task_stop(self):
        await self.runner.cleanup()
