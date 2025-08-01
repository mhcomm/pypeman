"""The REPL for the remoteadmin 'shell' plugin command.

This basically re-exposes the remote methods from :mod:`methods`
throuh a classical :class:`Cmd`.

The :class:`RemoteAdminShell` exported from this module has a gory
technical detail due to how the standard module :mod:`cmd` does not
work with async stuff at all.

The various shell command need to call out to async methods from
:mod:`methods`, and from a sync functions there are not that many ways:
    * :meth:`AbstractEventLoop.run_until_complete`,
    * :func:`asyncio.run` (which creates a loop and calls the above),
    * :func:`asyncio.run_coroutine_threadsafe`,
    * :func:`asyncio.to_thread`.

This last one is >=3.9 so unavailable (and not desirable in the context
of mutiple individual small calls).
`run_until_complete` (and by extension `asyncio.run`) *cannot be
nested!* However, at least in the main thread, the event loop is already
in running state (from all the way back in :func:`commands.amain`).

Eventually the solution is in the two following steps:
    * use :meth:`AbstractEventLoop.run_in_executor` from main thread,
    * re-delegate through :func:`asyncio.run_coroutine_threadsafe`.

    await asyncio.get_running_loop().run_in_executor(None, RemoteAdminShell(ws).cmdloop)

When :class:`PypemanShell` is instantiated, it stores a handle to the
current (main) thread's event loop. Then when comes the need to call
an async function it is sent as a task to said event loop and we block
here until resolved (see :func:`_sync`).

    return asyncio.run_coroutine_threadsafe(afn(self, *a, **ka), self._main_thread_loop).result(5)

Overall this might seems like a lot but Python isn't the only one that
couldn't figure out async correctly in time and messed up, so this one
I forgive.
"""

from __future__ import annotations

import asyncio
from cmd import Cmd
from functools import wraps
from shlex import split as arg_split
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
            self.stdout.write(f"{type(e).__name__}: {str(e) or '(no detail)'}.\n")

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
        # for commands that requires it
        self._current_channel: str | None = None
        # used with completion; this is set lazily but unconditionally:
        #   * if you list the channels (do_channels), result is stored
        #   * if you <tab> where a channel name would be expected, they are fetched implicitely
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
    async def do_list(self, channelname: str, search: str):
        """List messages of the selected channel.

        [..]
        [to pass args and what not, syntax accepted]

        This is a mostly direct call to the channel's message store's
        `search` (:meth:`MessageStore.search`) and as such will accept
        mostly the same arguments. The call is done through the same
        means as the HTTP API point `"/channels/{channelname}/messages"`
        (see :func:`methods.list_msgs`), so the only hiccup is the
        handling of meta-based searches; these need to use `'meta_'`.

        Examples:
            (Cmd) ...
        [..]

        TODO(wip)
        """
        kwargs: dict[str, str] = {}

        for arg in arg_split(search):
            name, found, val = arg.partition("=")
            if found:
                kwargs[name] = val
                continue

            # otherwise it's a positional arg; these are in order: start, end and order_by
            if "start" not in kwargs:
                kwargs["start"] = arg
            elif "end" not in kwargs:
                kwargs["end"] = arg
            elif "order_by" not in kwargs:
                kwargs["order_by"] = arg
            else:
                self.stdout.write(f"The meaning of {arg!r} is not clear, it will not be use.")
            # TODO: XXX: (about above)
            #   `start`/`stop` are no longer valid and as such will be removed
            #   (I added this part of the code mechanically-)
            #   `order_by` is I guess a valid positional argument;
            #   what about having `start_dt`/`end_dt` instead of `start`/`stop`?

        result = await methods.list_msgs(channelname=channelname, **kwargs)
        if not result["total"]:
            self.stdout.write("No message yet.\n")
            return
        messages = result["messages"]

        if isinstance(messages, list):
            for msg in messages:
                self.stdout.write(f"{msg['timestamp']} {msg['id']} {msg['state']}\n")
            if not messages:
                self.stdout.write("No matching message.\n")
            else:
                self.stdout.write(f"Matched {messages} message(s).\n")

        elif isinstance(messages, dict):
            total = 0
            for group in messages:
                self.stdout.write("Group {group!r}:\n")
                for msg in messages[group]:
                    self.stdout.write(f"\t{msg['timestamp']} {msg['id']} {msg['state']}\n")
            if not messages:
                self.stdout.write("No matching message.\n")
            else:
                self.stdout.write(f"Matched {total} message(s) into {len(messages)} group(s).\n")

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_replay(self, channelname: str, ids: str):
        """Replay a message list by their ids."""
        assert not "implemented"

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_view(self, channelname: str, ids: str):
        """View content of a message list by their ids."""
        assert not "implemented"

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_preview(self, channelname: str, ids: str):
        """Preview content of a message list by their ids."""
        assert not "implemented"

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_push(self, channelname: str, content: str):
        """Inject message with text as payload for selected channel."""
        assert not "implemented"
