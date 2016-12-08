import importlib
import traceback
import sys

def load_class(module, class_, deps):
    """
    Try to load a class if all deps can be imported.
    :param module: path to module contains class
    :param class_:
    :param deps:
    :return:
    """
    try:
        mod = importlib.import_module(module)
        return getattr(mod, class_)
    except ImportError as exc:
        traceback.print_exc()
        msg = str(exc)

        # Try to find any dependency in message
        found = False
        for dep in deps:
            if dep in msg:
                found = True
                break

        if not found:
            print("IMPORT ERROR...", file=sys.stderr)
            raise

        print("%s module not activated" % module)
        return None


def load(selfmodname, module, class_, dep=None):
    """
    load a class and add it to selfmodname namespace.
    :param selfmodname:
    :param module:
    :param class_:
    :param dep:
    :return:
    """
    if dep is None:
        dep = []

    selfmod = importlib.import_module(selfmodname)

    def init(*args, **kwargs):
        C = load_class(module, class_, dep)
        setattr(selfmod, class_, C)
        return C(*args, **kwargs)

    return init


class Wrapper(object):
    def __init__(self, wrapped):

        self._wrapped = sys.modules[wrapped]
        self._extra = {}

        self.__dict__.update(self._wrapped.__dict__)

        sys.modules[wrapped] = self

    def add_lazy(self, module, name, deps):
        self._extra[name] = (module, deps)

    def __getattr__(self, name):
        try:
            return getattr(self._wrapped, name)
        except AttributeError:
            if name in self._extra:
                setattr(self._wrapped, name, load_class(self._extra[name][0], name, self._extra[name][1]))
                return getattr(self._wrapped, name)
            else:
                raise
