"""TODO: doc
"""

import logging.config
from logging import getLogger
from importlib import import_module

from . import default_settings

# XXX: @rem channels
SETTINGS_IMPORTED = False

logger = getLogger(__name__)


class ConfigError(ImportError):
    """Pypeman conf error, likely due to `settings.py` not being found."""


class Settings:
    """Pypeman project-level (ie global as fuck) settings.

    Loosely inspired by the beautiful and influential
    `django.conf.settings` that we all came to love :gun:.

    Only names with an uppercase letter in A-Z will ever be considered.
    """

    def __init__(self):
        pass

    def __getattr__(self, name: str):
        """Get a setting value.

        This wrapper implements lazy initialisation of the `settings`
        global object. It is needed for 2 reasons:
            * constructor must not do any work so as to keep me sane;
            * sys.path can be wrong (see :func:`pypeman.commands.main`).
        """
        global SETTINGS_IMPORTED  # XXX(wip)
        if not self:
            self.__dict__.update(p for p in vars(default_settings).items() if "A" <= p[0][0] <= "Z")

            try:
                settings_mod = self.__dict__["_settings_mod"] = import_module(self.SETTINGS_MODULE)
                self.__dict__.update(p for p in vars(settings_mod).items() if "A" <= p[0][0] <= "Z")
                if self.__dict__.get("RETRY_STORE_PATH") is None:
                    logger.warning(
                        "No RETRY_STORE_PATH in settings, retry store unavailable."
                        + " (You may want to change this.)"
                    )

                SETTINGS_IMPORTED = True  # XXX(wip)

            except BaseException as e:
                self.__dict__["_loading_exc"] = e

            logging.config.dictConfig(self.LOGGING)
            return super().__getattribute__(name)

        raise AttributeError(f"type object '{type(self).__name__}' has no attribute '{name}'")

    def raise_for_missing(self):
        """If the user settings module couldn't be loaded, raise a ConfigError."""
        exc = self.__dict__.get("_loading_exc")
        if exc is not None:
            raise ConfigError(f"Cannot import setting module '{self.SETTINGS_MODULE}' (see above).") from exc

    def __bool__(self):
        """`settings` is truhty if it was imported (or attempted at all)."""
        return self.__dict__.get("_settings_mod") is not None or self.__dict__.get("_loading_exc") is not None

    def __setattr__(self, name, value):
        """This disallows mutating the settings (to some extent)."""
        # i'd like if it could be this way: ie no you dont modify settings at runtime _at all_
        raise TypeError(f"Settings are immutable (setting {name} to {value!r}).")


settings = Settings()
