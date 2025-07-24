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

    Such a plugin will almost always use :func:`asyncio.create_task`
    (& co, directly or not), and cancel it when asked.
    """

    @abstractmethod
    async def task_start(self):
        """Start the cooperative task.

        This needs to return in order for the whole pypeman processus
        to resume.
        """

    @abstractmethod
    async def task_stop(self):
        """Stop the cooperative task."""


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
