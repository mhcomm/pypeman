from __future__ import annotations

import asyncio
from cmd import Cmd
from functools import wraps
from typing import Awaitable
from typing import Callable

from aiohttp import ClientWebSocketResponse

from . import methods


def _sync(afn: Callable[..., Awaitable[object]]):
    """make an async sync, as :mod:`cmd` doesn't work with async"""

    @wraps(afn)
    def fn(self: RemoteAdminShell, *a: ..., **ka: ...):
        return asyncio.run_coroutine_threadsafe(afn(self, *a, **ka), self._main_thread_loop).result(5)

    return fn


def _with_current_channel(func: Callable[..., object]):
    """makes it required to have a channel selected"""

    @wraps(func)
    def wrapper(self: RemoteAdminShell, *a: ..., **ka: ...):
        if self._current_channel is None:
            self.stdout.write("You have to select a channel prior to using this command.\n")
            return
        return func(self, self._current_channel, *a, **ka)

    return wrapper


def _try_except_print(func: Callable[..., object]):
    """:mod:`cmd` simply doesn't handle failure at all"""

    @wraps(func)
    def wrapper(self: RemoteAdminShell, *a: ..., **ka: ...):
        try:
            return func(self, *a, **ka)
        except BaseException as e:
            self.stdout.write(f"{type(e).__name__}: {e or '(no detail)'}.\n")

    return wrapper


class RemoteAdminShell(Cmd):
    intro = "Welcome to the pypeman shell. Type help or ? to list commands.\n"

    @property
    def prompt(self):
        return f"pypeman({self._current_channel})> " if self._current_channel else "pypeman> "

    def __init__(self, ws: ClientWebSocketResponse, *a: ..., **ka: ...):
        super().__init__(*a, **ka)

        self._main_thread_loop = asyncio.get_event_loop()
        self._ws = ws
        self._current_channel: str | None = None
        self._avail_channels: set[str] | None = None

    def do_exit(self, _: str):
        return True

    do_EOF = do_exit

    @_try_except_print
    @_sync
    async def do_channels(self, _arg: str):
        """List available channels."""
        channels = await methods.list_channels(self._ws)
        self.stdout.write("\nChannel list:\n")
        for idx, channel in enumerate(channels):
            self.stdout.write(f"{idx}) {channel['short_name']} ({channel['status']})\n")
        self.stdout.write("\n")
        if self._avail_channels is None:
            self._avail_channels = {it["name"] for it in channels}

    @_try_except_print
    @_sync
    async def do_start(self, channelname: str):
        """Start a channel by its name, or the current one."""
        if self._current_channel and not channelname:
            channelname = self._current_channel
        res = await methods.start_channel(self._ws, channelname=channelname)
        self.stdout.write(f"{res['name']}: {res['status']}\n")

    @_try_except_print
    @_sync
    async def do_stop(self, channelname: str):
        """Stop a channel by its name, or the current one."""
        if self._current_channel and not channelname:
            channelname = self._current_channel
        res = await methods.stop_channel(self._ws, channelname=channelname)
        self.stdout.write(f"{res['name']}: {res['status']}\n")

    @_try_except_print
    @_sync
    async def do_select(self, channelname: str):
        """Select the channel to use for channel oriented commands."""
        if not channelname:
            self._current_channel = None
            return
        if self._avail_channels is None:
            channels = await methods.list_channels(self._ws)
            self._avail_channels = {it["name"] for it in channels}
        if channelname not in self._avail_channels:
            self.stdout.write(f"Warning: no channel name {channelname} on instance.\n")
        self._current_channel = channelname

    @_try_except_print
    @_sync
    async def complete_select(self, text: str, *_):
        if self._avail_channels is None:
            channels = await methods.list_channels(self._ws)
            self._avail_channels = {it["name"] for it in channels}
        return [name for name in self._avail_channels if name.startswith(text)]

    complete_start = complete_stop = complete_select

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_list(self, channel: str, search: str):
        """List messages of selected channel.

        You can specify start, end and order_by arguments
        Optional args:
            - to filter messages, you can pass start_dt and end_dt (isoformat datetimes)
            to filter messages
            - to filter messages, you can pass text and rtext (string between double quotes)
            to filter messages
        """
        assert not "implemented"

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_replay(self, channel: str, ids: str):
        """Replay a message list by their ids."""
        assert not "implemented"

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_view(self, channel: str, ids: str):
        """View content of a message list by their ids."""
        assert not "implemented"

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_preview(self, channel: str, ids: str):
        """Preview content of a message list by their ids."""
        assert not "implemented"

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_push(self, channel: str, content: str):
        """Inject message with text as payload for selected channel."""
        assert not "implemented"
