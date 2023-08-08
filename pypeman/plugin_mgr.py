"""
The Plugin Manager shall handle pypeman plugins.

The role of pypeman plugins is to allow inserting plugins, which don't really
make part of the pypeman graph, but are intended for debugging, monitoring
or any other code, that shall be present along the pypeman code.

It can also encourage development of plugins outside of pypeman, that can lateron
be merged into to pypeman.contrib.plugins or pypeman.plugins.
"""

import logging

from importlib import import_module

from pypeman.conf import settings


logger = logging.getLogger(__name__)


class PluginManager():
    def __init__(self):
        self.imported = False
        self.plugin_classes = []  # list of plugin modules
        self.plugins = []
        self.loop = None

    def set_loop(self, loop):
        self.loop = loop
        for plugin in self.plugins:
            plugin.set_loop(loop)

    def import_plugins(self):
        """
        import plugin modules and store the classes in a list
        """
        if self.imported:
            return
        for plugin_name in settings.PLUGINS:
            module_name, cls = plugin_name.rsplit(".", 1)
            module = import_module(module_name)
            plugin = getattr(module, cls)
            self.plugin_classes.append(plugin)

    def init_plugins(self):
        """
        initialize all plugins
        """
        # shutdown previous plugins if existing
        if self.plugins:
            for plugin in self.plugins:
                if plugin.status == plugin.STARTED:
                    plugin.do_stop()
            for plugin in self.plugins:
                if plugin.status == plugin.STOPPED:
                    plugin.do_destroy()

        # instantiate plugins
        self.plugins = []
        for plugin_cls in self.plugin_classes:
            self.plugins.append(plugin_cls())

    def ready_plugins(self):
        """
        call ready functions for all plugins.
        We know now, that *all* plugins are already
        instantiated
        """
        for plugin, plugin_cls in zip(self.plugins, self.plugin_classes):
            logger.info("calling ready for plugin %s", repr(plugin_cls))
            plugin.do_ready()

    def start_plugins(self):
        """
        call the start function for each plugin
        """
        loop = self.loop
        for plugin, plugin_cls in zip(self.plugins, self.plugin_classes):
            logger.info("starting plugin %s", repr(plugin_cls))
            loop.run_until_complete(plugin.do_start())
            # start whatever the plugin wants to start
            for to_start in plugin.to_start:
                loop.run_until_complete(to_start)

    def stop_plugins(self):
        """
        call the stop function for each plugin
        """
        loop = self.loop
        for plugin, plugin_cls in zip(self.plugins, self.plugin_classes):
            logger.info("stopping plugin %s", repr(plugin_cls))
            loop.run_until_complete(plugin.do_stop())


manager = PluginManager()
