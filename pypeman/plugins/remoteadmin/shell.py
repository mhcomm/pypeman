"""The REPL for the remoteadmin 'shell' plugin command.

This basically re-exposes the remote methods from :mod:`methods`
throuh a classical :class:`Cmd`. It is in essence a websocket client
to the websocket server in :mod:`url`.

---

The :class:`RemoteAdminShell` exported from this module has a gory
technical detail due to how the standard module :mod:`cmd` does not
work with async stuff at all.

The various shell commands need to call out to async methods from
:mod:`methods`, and from a sync functions there are not that many ways:
    * :meth:`AbstractEventLoop.run_until_complete`,
    * :func:`asyncio.run` (which creates a loop and calls the above),
    * :func:`asyncio.run_coroutine_threadsafe`,
    * :func:`asyncio.to_thread`.

This last one is >=3.9 so unavailable (and not desirable in the context
of mutiple individual small calls).
`run_until_complete` (and by extension `asyncio.run`) **cannot** be
nested! Moreover, at least in the main thread, the event loop is already
in the running state (from all the way back in :func:`commands.amain`).

Eventually the solution is with the two following steps:
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
from datetime import datetime
from functools import wraps
from shlex import split as arg_split
from typing import Awaitable
from typing import Callable

from aiohttp import ClientWebSocketResponse

from ...message import Message
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
        # also for completion; gathers any (store specific) id we see at all
        self._known_msg_ids: set[str] = set()

    def do_exit(self, _: str):
        "you know what it does"
        return True

    do_EOF = do_exit

    def _arg_extract_kwargs(self, arg: str, positional_names: list[str]):
        """Extracts named args, use by some commands.

        `arg` would be as passed to one of the `do_..` Cmd method.
        It is then split mostly like a shell command (eg quotes should
        work) and items of the form name='val' are gathered into
        a dict.

        If the command allows for positional arguments, these should
        be listed in order in `positional_names`.
        """
        kwargs: dict[str, str] = {}
        positional_names = list(positional_names)

        for arg in arg_split(arg):
            name, found, val = arg.partition("=")
            # if '=' is found then it's a kwargs pass as name='val'
            if found and name.isidentifier():
                kwargs[name] = val
            # otherwise it's maybe a positional arg; check if we have any of these left
            elif positional_names:
                kwargs[positional_names.pop(0)] = arg
            # no more positional arg; notify and disregard
            else:
                self.stdout.write(f"The meaning of {arg!r} is not clear, it will not be use.")

        return kwargs

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
        if not channelname:
            assert self._current_channel, "no channel given nor selected"
            channelname = self._current_channel
        res = await methods.start_channel(self._ws, channelname=channelname)
        self.stdout.write(f"{res['name']}: {res['status']}\n")

    @_try_except_print
    @_sync
    async def do_stop(self, channelname: str):
        """Stop a channel by its name, or the current one."""
        if not channelname:
            assert self._current_channel, "no channel given nor selected"
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

    @_sync
    async def complete_select(self, text: str, *_: ...):
        "channel name completion, unconditionally fetches available"
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

        This is a mostly direct call to the channel's message store's
        `search` (:meth:`MessageStore.search`). It will accept mostly
        the same arguments. 'meta'-based searches must use `'meta_..'`.

        Examples:
            (Cmd) select my_channel
            (Cmd) list start_dt='2025-10-10 15:30' end_dt='2025-10-10 17' order_by=timestamp
            (Cmd) list start_dt=2024 end_dt=2025 group_by=status
        """
        kwargs = self._arg_extract_kwargs(search, ["start", "end", "order_by"])
        # TODO: XXX: (about above)
        #   `start`/`stop` are no longer valid and as such will be removed
        #   (I added this part of the code mechanically, but...)
        #   `order_by` is I guess a valid positional argument;
        #   what about having `start_dt`/`end_dt` instead of `start`/`stop`?

        result = await methods.list_msgs(self._ws, channelname=channelname, **kwargs)
        if not result["total"]:
            self.stdout.write("No message yet.\n")
            return
        messages = result["messages"]

        if isinstance(messages, list):
            for msg in messages:
                self.stdout.write(f"{msg['timestamp']} {msg['id']} {msg['state']}\n")
                self._known_msg_ids.add(msg["id"])
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
                    self._known_msg_ids.add(msg["id"])
            if not messages:
                self.stdout.write("No matching message.\n")
            else:
                self.stdout.write(f"Matched {total} message(s) into {len(messages)} group(s).\n")

    def complete_list(self, text: str, *_: ...):
        "search args completion, only provides ids that were 'list'-ed"
        if text in {"start_dt=", "end_dt="}:
            # life hack: if you <tab> at this point, we'll give you the current time
            return [datetime.now().strftime("'%Y-%m-%d %H:%M'")]

        if "start_id=" == text:
            # life hack 2: we try to be useful mdr
            return [id for id in self._known_msg_ids if id.startswith(text)]

        flate = {"count=", "start_id=", "start_dt=", "end_dt=", "text=", "rtext=", "meta_"}
        ordre = {f"order_by={v}" for v in {"timestamp", "state", "-timestamp", "-state"}}
        grope = {f"group_by={v}" for v in {"state"}}
        return [w for w in {*flate, *ordre, *grope} if w.startswith(text)]

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_replay(self, channelname: str, ids: str):
        """Replay a list of messages by their message store ids."""
        msg_ids = str(ids).split()
        failures = 0

        for msg_id in msg_ids:
            try:  # catch individual failures
                msg_dict = await methods.replay_msg(self._ws, channelname=channelname, message_id=msg_id)
            except BaseException as e:
                self.stdout.write(f"On message {msg_id} - {type(e)}: {str(e) or '(no detail)'}\n")
                failures += 1
                continue

            self.stdout.write(f"(Store id {msg_id}) - resulting message:\n")
            self.stdout.write(Message.from_dict(msg_dict).to_print())
            self.stdout.write("\n")

        if 1 < len(msg_ids):
            self.stdout.write(f"Summary: {len(msg_ids)-failures}/{len(msg_ids)} ok ({failures} failures)\n")

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_view(self, channelname: str, ids: str):
        """View a list of messages by their message store ids."""
        # don't asyncio.gather here it's useless;
        # the proper thing to do would be https://www.jsonrpc.org/specification#batch
        # (+7 lines in .urls when i did) but it's unnecessary trust me sis/bro
        for msg_id in str(ids).split():
            msg_dict = await methods.view_msg(self._ws, channelname=channelname, message_id=msg_id)
            self.stdout.write(f"(Store id {msg_id})\n")
            self.stdout.write(Message.from_dict(msg_dict).to_print())
            self.stdout.write("\n")

    def complete_replay(self, text: str, *_: ...):
        "message store id completion, can only provides 'list'-ed ones"
        return [id for id in self._known_msg_ids if id.startswith(text)]

    complete_view = complete_replay

    @_try_except_print
    @_with_current_channel
    @_sync
    async def do_push(self, channelname: str, content: str):
        """Inject a message for handling by the selected channel.

        The first positional argument will be the message's payload.
        To additionally specify meta for the message, add a meta='val'
        argument; val must be a valid json object (ie python dict).

        Example:
            (Cmd) select your_channel
            (Cmd) push meta='{"yey": "ok"}' aaaaaaaaaaaaa
        """
        kwargs = self._arg_extract_kwargs(content, ["payload"])

        payload = kwargs["payload"]
        meta = kwargs.get("meta", "")

        msg_dict = await methods.push_msg(self._ws, channelname=channelname, payload=payload, meta=meta)
        self.stdout.write(Message.from_dict(msg_dict).to_print())

    def complete_push(self, text: str, *_):
        "s'more completion"
        return [w for w in {"payload=", "meta="} if w.startswith(text)]
