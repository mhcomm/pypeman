"""Plugin manager module.

Importing this module constructs :obj:`manager` (this is a no-op).

Pypeman's plugin manager has a straightforward use:
    * :meth:`PluginManager.register_plugins` any times;
    * until :meth:`PluginManager.instantiate_plugins` once;
    * then :meth:`PluginManager.get_plugins` to select by mixin.

Every plugins must derive from :class:`BasePlugin`. However by itself
it will not bring anything (its constructor is called and that's it).

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
    def __init__(self):
        # a list, not a set: registration order is the command order
        self._registered_classes: list[type[BasePlugin]] = []
        self._instances: list[BasePlugin] | None = None

    def register_plugins(self, *plugins: str):
        """Register one or more plugins.

        This operation is invalid after :meth:`instantiate_plugins`.
        """
        assert self._instances is None, f"invalid operation: late plugin registery {plugins}"

        for plugin_path in plugins:
            module_name, _, cls_name = plugin_path.rpartition(".")
            module = import_module(module_name)
            cls = getattr(module, cls_name)
            # being extra-defensive here as we are importing blind
            assert isinstance(cls, type) and BasePlugin in cls.mro(), f"{cls!r} is not a plugin class"
            if cls not in self._registered_classes:
                self._registered_classes.append(cls)

    def instantiate_plugins(self):
        """Instantiate the various plugin classes.

        This /only/ instantiate! No plugin-specific thing is performed
        outside of potential `__init__`s.

        This operation becomes invalid after it has been performed once.
        """
        assert self._instances is None, "invalid operation: instantiate called again"
        self._instances = [cls() for cls in self._registered_classes]

    _MixinTypeVar_ = TypeVar("_MixinTypeVar_", bound=MixinClasses_)

    def get_plugins(self, of_type: type[_MixinTypeVar_]):
        """Retrieve all the plugin instances of a certain type.

        This is of course incorrect until :meth:`instantiate_plugins`.
        """
        assert self._instances is not None, "invalid operation: instantiate not called"
        return (it for it in self._instances if isinstance(it, of_type))

    def get_all_plugins(self):
        """Retrieve all the plugin instances. See also `get_plugins`.

        This is of course incorrect until :meth:`instantiate_plugins`.
        """
        assert self._instances is not None, "invalid operation: instantiate not called"
        return iter(self._instances)


manager = PluginManager()
