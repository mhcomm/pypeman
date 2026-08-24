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

from pypeman.conf import settings


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


class _WebappBundle:
    """The shared aiohttp app of :class:`BundledWebappPluginMixin`.

    Every bundled plugin registers itself at instantiation; the first
    `task_start` builds the app (mounting every member's sub-app under
    its prefix) and starts it exactly once. host/port come from
    `settings.WEBAPP_PLUGINS_CONFIG`.
    """

    def __init__(self):
        self._members: list[BundledWebappPluginMixin] = []
        self._runner: web.AppRunner | None = None
        self._started = False

    def register(self, plugin: BundledWebappPluginMixin):
        self._members.append(plugin)

    async def start_once(self):
        if self._started:
            return
        # no await between the check and the set: atomic on the loop
        self._started = True

        app = web.Application()
        for plugin in self._members:
            prefix = plugin.webapp_prefix()
            if not prefix.startswith("/") or "/" == prefix:
                raise ValueError(
                    f"{type(plugin).__name__}.webapp_prefix() must start with '/'"
                    + f" and not be '/' (got {prefix!r})"
                )
            app.add_subapp(prefix, plugin.webapp_urls())

        conf = settings.WEBAPP_PLUGINS_CONFIG
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, str(conf["host"]), int(conf["port"]))
        await site.start()

    async def stop_once(self):
        runner, self._runner = self._runner, None
        if runner is not None:
            await runner.cleanup()

    def _reset(self):
        """Test hook: forget members and started state."""
        self._members = []
        self._runner = None
        self._started = False


webapp_bundle = _WebappBundle()


class BundledWebappPluginMixin(TaskPluginMixin, ABC):
    """Mixin to integrate with the common webapp.

    This mixin extends on :class:`TaskPluginMixin` (so if your plugin
    is implementing it, there is no need to include both in the base
    mixins, and you will need to follow cooperative inheritance --
    in particular call `super().__init__()`).

    The idea is to bundle together every plugins which may want to
    expose web API endpoints. By using this mixin, the implementing
    plugin registers itself to be part of the bundle. All bundled
    plugins are served by a single web app on the same host and port
    (configured through `settings.WEBAPP_PLUGINS_CONFIG`), each under
    its own non-optional URL prefix (:meth:`webapp_prefix`).

    This is of course an optional approach as no plugin is prevented
    from spinning up its own web app. Furthermore, only enabled plugins
    will actually be bundled within the common webapp.
    """

    def __init__(self):
        super().__init__()
        webapp_bundle.register(self)

    @abstractmethod
    def webapp_prefix(self) -> str:
        """The URL prefix this plugin is mounted under.

        Must start with '/' and not be just '/'.
        """

    @abstractmethod
    def webapp_urls(self) -> web.Application:
        """Return a fresh sub-application with the plugin's routes.

        The routes are relative to :meth:`webapp_prefix`.
        """

    async def task_start(self):
        await webapp_bundle.start_once()

    async def task_stop(self):
        await webapp_bundle.stop_once()


MixinClasses_ = Union[
    CommandPluginMixin,
    TaskPluginMixin,
    BundledWebappPluginMixin,
]
