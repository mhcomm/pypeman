"""See package-level documentation."""

import asyncio
from argparse import ArgumentParser
from argparse import Namespace
from logging import getLogger

from aiohttp import ClientSession
from aiohttp import web

from pypeman.conf import settings
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import BundledWebappPluginMixin
from pypeman.plugins.base import CommandPluginMixin
from pypeman.plugins.remoteadmin.shell import RemoteAdminShell
from pypeman.plugins.remoteadmin.urls import make_routes

logger = getLogger(__name__)


class RemoteAdminPlugin(BasePlugin, CommandPluginMixin, BundledWebappPluginMixin):
    """Provides the `shell` command, alongside the related server."""

    def __init__(self):
        # only true when the USER settings module defines it
        # (it is no longer part of the defaults)
        if hasattr(settings, "REMOTE_ADMIN_WEBSOCKET_CONFIG"):
            logger.warning(
                "REMOTE_ADMIN_WEBSOCKET_CONFIG and REMOTE_ADMIN_WEB_CONFIG are deprecated:"
                + " use REMOTE_ADMIN_CONFIG['url'] for the prefix"
                + " and WEBAPP_PLUGINS_CONFIG for host/port."
            )
            conf = settings.REMOTE_ADMIN_WEBSOCKET_CONFIG
        else:
            conf = settings.REMOTE_ADMIN_CONFIG
            if "host" in conf or "port" in conf:
                logger.warning(
                    "REMOTE_ADMIN_CONFIG host/port are ignored;"
                    + " the shared webapp uses WEBAPP_PLUGINS_CONFIG."
                )
        # default is no prefix (served at the root of the shared app);
        # rstrip so that the legacy '/' still means the root
        self._url_prefix = str(conf.get("url") or "").rstrip("/")

        super().__init__()  # registers into the webapp bundle

    @classmethod
    def command_name(cls):
        return "shell"

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument("host", nargs="?", help="override settings' host")
        parser.add_argument("port", nargs="?", help="override settings' port", type=int)

    async def command(self, options: Namespace):
        settings.raise_for_missing()
        conf = settings.WEBAPP_PLUGINS_CONFIG
        host = options.host or str(conf["host"])
        port = options.port or int(conf["port"])
        url = f"ws://{host}:{port}{self._url_prefix}/"
        async with ClientSession() as cs, cs.ws_connect(url) as ws:
            # no other way to make it work with python's cmd module...
            # (see :mod:`shell`, it has doc about that)
            await asyncio.get_running_loop().run_in_executor(None, RemoteAdminShell(ws).cmdloop)

    def webapp_prefix(self) -> str:
        return self._url_prefix

    def webapp_urls(self) -> list[web.RouteDef]:
        # routes are relative: the bundle mounts them at webapp_prefix()
        return make_routes()
