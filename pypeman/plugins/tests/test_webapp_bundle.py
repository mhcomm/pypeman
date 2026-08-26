"""Tests for the shared webapp bundle of BundledWebappPluginMixin."""

import asyncio

import pytest
from aiohttp import ClientSession
from aiohttp import web

from pypeman.conf import settings
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import BundledWebappPluginMixin
from pypeman.plugins.base import _WebappBundle
from pypeman.plugins.base import webapp_bundle


def _mk_plugin_cls(prefix):
    class DummyPlugin(BasePlugin, BundledWebappPluginMixin):
        """A bundled plugin answering 'ok <prefix>' at <prefix>/ping."""

        def webapp_prefix(self):
            return prefix

        def webapp_urls(self):
            async def ping(_request):
                return web.Response(text=f"ok {prefix}")

            return [web.get("/ping", ping)]

    return DummyPlugin


@pytest.fixture
def bundle_env(monkeypatch):
    """Fresh global bundle + a WEBAPP_PLUGINS_CONFIG on a free port."""
    monkeypatch.setitem(
        settings.__dict__, "WEBAPP_PLUGINS_CONFIG", {"host": "127.0.0.1", "port": 0}
    )
    webapp_bundle._reset()
    yield webapp_bundle
    webapp_bundle._reset()


def test_two_plugins_one_app(bundle_env):
    async def scenario():
        one = _mk_plugin_cls("/one")()
        two = _mk_plugin_cls("/two")()

        # every member's task_start delegates to the same bundle;
        # gathering both must start the web app exactly once
        await asyncio.gather(one.task_start(), two.task_start())
        assert bundle_env._runner is not None

        host, port = bundle_env._runner.addresses[0][:2]
        async with ClientSession() as cs:
            for prefix in ("/one", "/two"):
                async with cs.get(f"http://{host}:{port}{prefix}/ping") as resp:
                    assert resp.status == 200
                    assert await resp.text() == f"ok {prefix}"

        # both members stop it, cleanup must run exactly once and not fail
        await asyncio.gather(one.task_stop(), two.task_stop())
        assert bundle_env._runner is None

    asyncio.run(scenario())


def test_empty_prefix_mounts_at_root(bundle_env):
    async def scenario():
        root = _mk_plugin_cls("")()
        sub = _mk_plugin_cls("/sub")()
        await root.task_start()

        host, port = bundle_env._runner.addresses[0][:2]
        async with ClientSession() as cs:
            async with cs.get(f"http://{host}:{port}/ping") as resp:
                assert resp.status == 200
                assert await resp.text() == "ok "
            async with cs.get(f"http://{host}:{port}/sub/ping") as resp:
                assert resp.status == 200
                assert await resp.text() == "ok /sub"

        await root.task_stop()
        await sub.task_stop()

    asyncio.run(scenario())


def test_stop_without_start_is_a_noop(bundle_env):
    plugin = _mk_plugin_cls("/lonely")()
    asyncio.run(plugin.task_stop())  # must not raise


def test_invalid_prefix_rejected(bundle_env):
    async def scenario():
        for bad_prefix in ("/", "oops"):
            bundle = _WebappBundle()
            bundle.register(_mk_plugin_cls(bad_prefix)())
            with pytest.raises(ValueError):
                await bundle.start_once()

    asyncio.run(scenario())
