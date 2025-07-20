from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from argparse import ArgumentParser
from argparse import Namespace
from typing import Union


class BasePlugin(ABC):
    """

    Individual implementing classes should have a docstring.
    If will be used as help in various places (for example CLI).
    """


class CommandPluginMixin(ABC):
    """Mixin for a plugin that should be made available from the CLI.

    For example:

        class HelloPlugin(BasePlugin, CommandPluginMixin): ...

        $ pypeman hello ...
    """

    @classmethod
    def command_name(cls) -> str:
        """"""
        # if you're here after 3.9,
        # please change it for a simple `return cls...removesuffix("plugin")`
        name = cls.__name__.lower()
        if name.endswith("plugin"):
            name = name[: -len("plugin")]
        return name

    @classmethod
    @abstractmethod
    def command_parse(cls, parser: ArgumentParser):
        """"""

    @abstractmethod
    async def command(self, options: Namespace):
        """"""


class TaskPluginMixin(ABC):
    """Mixin for a plugin with an async task.

    Such a plugin will almost always use :func:`asyncio.create_task`
    (& co, directly or not), and stop/cancel/shutdown it when asked.
    """

    @abstractmethod
    async def task_start(self):
        """"""

    @abstractmethod
    async def task_stop(self):
        """"""


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
