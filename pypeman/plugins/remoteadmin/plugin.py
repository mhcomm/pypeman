"""See package-level documentation."""

from __future__ import annotations

from argparse import ArgumentParser
from argparse import Namespace
from logging import getLogger

from aiohttp import web

from ...conf import settings
from ..base import BasePlugin
from ..base import CommandPluginMixin
from ..base import TaskPluginMixin
from . import urls

logger = getLogger(__name__)


class RemoteAdminPlugin(BasePlugin, CommandPluginMixin, TaskPluginMixin):
    """Provides the `shell` command, alongside the related server."""

    def __init__(self):
        if hasattr(settings, "REMOTE_ADMIN_WEBSOCKET_CONFIG"):
            logger.warning(
                "REMOTE_ADMIN_WEBSOCKET_CONFIG and REMOTE_ADMIN_WEB_CONFIG are deprecated,"
                + " please use REMOTE_ADMIN_CONFIG"
            )
            conf = settings.REMOTE_ADMIN_WEBSOCKET_CONFIG
        else:
            conf = settings.REMOTE_ADMIN_CONFIG
        self.host = str(conf["host"])
        self.port = int(conf["port"])
        self.url_prefix = str(conf.get("url", ""))

        """
        # old veryold:
        remote = remoteadmin.RemoteAdminServer(loop=loop,
                  **settings.REMOTE_ADMIN_WEBSOCKET_CONFIG)
        webadmin = remoteadmin.WebAdmin(loop=loop,
                  **settings.REMOTE_ADMIN_WEB_CONFIG)

        # old plugin:
        plugins were all started with `cls()`
        __init__(self, host="127.0.0.1", port=8091, url_prefix="")
        and the `shell` command:
        remoteadmin.PypemanShell(url='ws://%s:%s'
                  % (settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['host'],
                     settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['port'])
              ).cmdloop()

        # TODO: being deprecated, see RemoteAdminPlugin's constructor
        REMOTE_ADMIN_WEBSOCKET_CONFIG = {
            "host": "localhost",
            "port": "8091",
            "ssl": None,
            "url": None,  # must be set when behind a reverse proxy
        }
        REMOTE_ADMIN_WEB_CONFIG = {
            "host": "localhost",
            "port": "8090",
            "ssl": None,
        }
        """

    @classmethod
    def command_name(cls):
        return "shell"

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument("host", nargs="?", help="override settings' host")
        parser.add_argument("port", nargs="?", help="override settings' port")

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
