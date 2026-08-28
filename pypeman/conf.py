"""The global settings module.

Importing this module constructs :obj:`settings` (this is a no-op).

Implements the loading and re-exporting of the user settings module.
Use the :obj:`pypeman.conf.settings` object to access these.

The settings are first initialized with :mod:`pypeman.default_settings`.
This is where resides the sole setting that cannot be set from a user
module: `settings.SETTINGS_MODULE`. Its default value is `"settings"`,
it can be changed with `$PYPEMAN_SETTINGS_MODULE`.

Loading of user settings is attempted once, lazily (ie on first
`__getattr__`). If it failed, :obj:`settings` will still fall back onto
the default values for as long as pypeman doesn't require user settings.
:meth:`Settings.raise_for_missing` is called when actual user settings
are eventually required.
"""

from __future__ import annotations

from importlib import import_module
from logging import getLogger
from os import environ
import logging.config

from pypeman import default_settings


logger = getLogger(__name__)


class ConfigError(ImportError):
    """Pypeman conf error, likely due to `settings.py` not being found."""


class Settings:
    """Pypeman project-level (ie global as fuck) settings.

    Loosely inspired by the beautiful and influential
    `django.conf.settings` that we all came to love :gun:.

    Only names with an uppercase letter in A-Z will ever be considered.
    """

    def __init__(self, module_name: str | None = None):
        self.__dict__["SETTINGS_MODULE"] = environ.get("PYPEMAN_SETTINGS_MODULE", "settings")
        if module_name is not None:
            self.__dict__["SETTINGS_MODULE"] = module_name

    def __getattr__(self, name: str):
        """Get a setting value.

        This wrapper implements lazy initialisation of the `settings`
        global object. It is needed for 2 reasons:
            * constructor must not do any work so as to keep me sane;
            * sys.path can be wrong (see :func:`pypeman.commands.main`).

        Reminder on python datamodel: this is only called when `name`
        isn't present on the object, ie either:
            * not loaded yet or;
            * not present at all.
        """
        if self:  # already loaded: this is the 'not present at all' case
            raise AttributeError(f"object '{type(self).__name__}' has no attribute '{name}'")
        self.init_settings()
        return super().__getattribute__(name)

    def init_settings(self):
        """Actually performs the import.

        You do not need to do this manually in normal operation,
        settings can just be accessed.

        This **can** be used to force-update the settings from the
        ``SETTINGS_MODULE`` (typ. for testing). This will not raise
        on failure; use `raise_for_missing` for this:

            SETTINGS_MODULE = 'tests.prout'
            conf.settings.init_settings()
            conf.settings.raise_for_missing()
        """
        # save this before clearing; we'd want to clear so as to now muddy things up
        settings_impat = str(self.__dict__["SETTINGS_MODULE"])
        self.__dict__.clear()
        self.__dict__.update(p for p in vars(default_settings).items() if "A" <= p[0][0] <= "Z")

        try:
            settings_mod = self.__dict__["_settings_mod"] = import_module(settings_impat)
            self.__dict__.update(p for p in vars(settings_mod).items() if "A" <= p[0][0] <= "Z")
            if self.__dict__.get("RETRY_STORE_PATH") is None:
                logger.warning(
                    "No RETRY_STORE_PATH in settings, retry store unavailable."
                    + " (You may want to change this.)"
                )
                # make it at least be present, even if none;
                # some code migh rely on this i haven't checked
                self.RETRY_STORE_PATH = None

        except BaseException as e:
            self.__dict__["_loading_exc"] = e

        logging.config.dictConfig(self.LOGGING)

    def raise_for_missing(self):
        """Raise :exc:`ConfigError` if the user settings module
        couldn't be loaded."""
        exc = self.__dict__.get("_loading_exc")
        if exc is not None:
            raise ConfigError(f"Cannot import setting module '{self.SETTINGS_MODULE}' (see above).") from exc

    def __bool__(self):
        """Truhty if it was imported (or attempted at all)."""
        return self.__dict__.get("_settings_mod") is not None or self.__dict__.get("_loading_exc") is not None

    def __setattr__(self, name, value):
        """This disallows mutating the settings (to some extent)."""
        raise TypeError(f"Settings are immutable (setting {name} to {value!r}).")


settings = Settings()
