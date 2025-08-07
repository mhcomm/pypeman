"""Abstract classes for plugins.

All plugins must inherit :class:`BasePlugin` to be considered.

See the description of the individual mixin classes in this module.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from argparse import ArgumentParser
from argparse import Namespace
from typing import Union

from aiohttp import web

from ..conf import settings


class BasePlugin(ABC):
    """plug-and-play!

    Individual implementing classes should have a docstring.
    It will be used as help in various places (for example CLI).
    """


class CommandPluginMixin(ABC):
    """Mixin for a plugin that should be made available from the CLI.

    For example:

        class HelloPlugin(BasePlugin, CommandPluginMixin): ...

        $ pypeman hello ...
    """

    @classmethod
    def command_name(cls) -> str:
        """Returns the name of the command at the CLI.

        By default, this will be the name of the class, barring
        any "Plugin" suffix. By convention and to match this behavior,
        it should be alphabetic lowercase [a-z] only (no '-' or '_').
        """
        # if you're here after 3.9,
        # please change it for a simple `return cls...removesuffix("plugin")`
        name = cls.__name__.lower()
        if name.endswith("plugin"):
            name = name[: -len("plugin")]
        return name

    @classmethod
    @abstractmethod
    def command_parse(cls, parser: ArgumentParser):
        """Register options and arguments."""

    @abstractmethod
    async def command(self, options: Namespace):
        """Execute the actual command.

        `options` results from the parser which was passed to
        :meth:`command_parse`.

        Once this method returns, the python interpreter exit.

        Note that at this point the user project is not loaded,
        and the user settings module might have not been loaded;
        if the command requires actual user settings,
        :meth:`Settings.raise_for_missing` must be called once.
        """


class TaskPluginMixin(ABC):
    """Mixin for a plugin with an async task.

    All task plugins are started (:meth:`task_start`) at the start of
    pypeman, after loading the user project but before kicking off the
    endpoints and channels.

    Such a plugin thus needs to be cooperative: if :meth:`task_start`
    takes a long time to return, it will hang everything else.

    The usual approach is to use :func:`asyncio.create_task` (& co,
    directly or not), and cancel it when asked. This can also be a way
    for a plugin to have code run before project kick-off or after
    project shutdown without it necessarily being a background task.
    """

    @abstractmethod
    async def task_start(self):
        """Start the cooperative task.

        Called before the project is started but after it is loaded.
        That is the user settings and the pypeman graph are available,
        but the channel and endpoints are not started yet.

        This needs to return in order for the whole pypeman processus
        to resume.
        """

    @abstractmethod
    async def task_stop(self):
        """Stop the cooperative task.

        Called after every channel and endpoint is fully stopped.
        """


class BundledWebappPluginMixin(TaskPluginMixin, ABC):
    """i don like it, but theres my proposal for it anyways

    This mixin extends on :class:`TaskPluginMixin` (so if your plugin
    is implementing it, there is no need to include both in the base
    mixins).

    The idea is to bundle together every plugins which may want to
    expose web API endpoints. By using this mixin, the implementing
    plugin essentially registers itself to be part of the bundle.
    It will be served under the same port (configured through
    `settings.WEBAPP_PLUGINS_CONFIG`).

    This is of cours an optional approach as no plugin is prevented
    from spinning up its own web app.

    Side note: an implementing plugin that wants to also use
    :meth:`task_start`/:meth:`task_stop` **must** use `super()`.

    ;

    WIP: this is not hooked in, not use yet;
    RemoteAdminPlugin would inherit from this mixin rather than
    TaskPluginMixin directly (making it shorted by, like, 5 lines)
    HOWEVER this will be done under the condition that we make
    prefix **non optional**
    mhclient/app/interface-interop/interface-interop-service.js
    """

    @classmethod
    @abstractmethod
    def webapp_prefix(cls) -> str:
        "..."

    @abstractmethod
    def webapp_urls(self) -> web.Application:
        "..."

    __bundle: web.Application
    __runner: web.AppRunner

    async def task_start(self):
        __class__.__bundle = web.Application()
        __class__.__bundle.add_subapp(self.webapp_prefix(), self.webapp_urls())

        conf = settings.WEBAPP_PLUGINS_CONFIG
        host = str(conf["host"])
        port = int(conf["port"])

        __class__.__runner = web.AppRunner(__class__.__bundle)
        await __class__.__runner.setup()
        site = web.TCPSite(__class__.__runner, host, port)
        await site.start()

    async def task_stop(self):
        await __class__.__runner.cleanup()


# class ChannelEventPluginMixin(ABC):
#
#     @abstractmethod
#     async def _channel_started(self, chan: BaseChannel):
#         """"""
#
#     @abstractmethod
#     async def _channel_stopped(self, chan: BaseChannel):
#         """"""


MixinClasses_ = Union[
    CommandPluginMixin,
    TaskPluginMixin,
]
