"""Plugin manager module.

Importing this module constructs :obj:`manager` (this is a no-op).

Pypeman's plugin manager has a straightforward use:
    * :meth:`PluginManager.register_plugins` any times;
    * until :meth:`PluginManager.instantiate_plugins` once;
    * then :meth:`PluginManager.get_module` to select by mixin.

Every plugins must derive from :class:`BasePlugin`. However by itself
it will not bring anything (its constructor is called..).

The mixin classes defined in :mod:`pypeman.plugins.base` make it
possible to hook in at various stages of the application's life cycle.
"""

from __future__ import annotations

from importlib import import_module
from logging import getLogger
from typing import TypeVar

from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import MixinClasses_


logger = getLogger(__name__)


class PluginManager:
    _registered_classes: set[type[BasePlugin]]
    _instances: list[BasePlugin]

    def __init__(self):
        self._registered_classes = set()
        self._instances = []

    def register_plugins(self, *plugins: str):
        """Register one or more plugins.

        This operation becomes invalid after :meth:`instantiate_plugins`.
        """
        assert not self._instances, f"invalid operation: late plugin registery {plugins}"

        for plugin_path in plugins:
            module_name, _, cls_name = plugin_path.rpartition(".")
            module = import_module(module_name)
            cls = getattr(module, cls_name)
            # being extra-defensive here as we are importing blind
            assert isinstance(cls, type) and BasePlugin in cls.mro(), f"{cls!r} is not a plugin class"
            # if cls in self._registered_classes:
            #     # should it just be 'info', if even anything at all actually
            #     logger.warning(f"Plugin registed twice: {cls} (now as {plugin_path})")
            self._registered_classes.add(cls)

    def instantiate_plugins(self):
        """Instantiate the various plugin classes.

        This /only/ instantiate! No plugin-specific thing is performed
        outside of potential `__init__`s.

        This operation becomes invalid after it has been performed once.
        """
        assert not self._instances, "invalid operation: instantiate called again"

        self._instances = [cls() for cls in self._registered_classes]

    _MixinTypeVar_ = TypeVar("_MixinTypeVar_", bound=MixinClasses_)

    def get_plugins(self, of_type: type[_MixinTypeVar_]):
        """Retrieve all the plugin instances of a certain type.

        This is of course incorrect until :meth:`instantiate_plugins`.
        """
        return (it for it in self._instances if isinstance(it, of_type))

    def get_all_plugins(self):
        """Retrieve all the plugin instances. See also `get_plugins`.

        This is of course incorrect until :meth:`instantiate_plugins`.
        """
        return iter(self._instances)


# TODO: me no liky, should at leat be proper singleton (same with settings one day)
# but this is the mentality of pypeman in many places, and it's making me question everything...
manager = PluginManager()

# meh, mostly didn't know where to put it but it makes sense here ig
# otherwise it would make more sense to have it in base_settings,
# but that requires that it's used correctly in project settings:
#   PLUGINS.extend([...])  rather than  PLUGINS = [...]
# CORE_PLUGINS = {
#     "pypeman.plugins.plugins.ListPluginsPlugin",
#     "pypeman.plugins.graph.GraphPlugin",
#     "pypeman.plugins.settings.PrintSettingsPlugin",
#     # i dont like the name 'startproject' cause it seems to me like that
#     # would be an alias for 'start' (or the other way around, you get it)
#     #"pypeman.plugins.project.StartProjectPlugin",
# }
