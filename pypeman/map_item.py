import time
import logging

logger = logging.getLogger(__name__)

class MapItem:
    def __init__(self, old=None, new=None, default=None, transform=None):
        self.old = old
        self.new = new if new else old
        if callable(transform):
            self.transform = transform
        else:
            self.transform = lambda x, y:x
        self.default = default

    def conv(self, oldDict, newDict, msg):
        value = oldDict
        if self.old:
            for part in self.old.split('.'):
                if not value == None:
                    value = value.get(part)

            value = self.transform(value, msg)

        if (not self.old or not value) and self.default is not None:
            value = self.default

        dest = newDict
        parts = self.new.split('.')
        for part in parts[:-1]:
            if dest.get(part) == None:
                dest[part] = {}
            dest = dest[part]

        dest[parts[-1]] = value


class JoinMapItem(MapItem):
    def __init__(self, old, new, sep=''):
        self.sep = sep
        super().__init__(old, new)
    def conv(self, oldDict, newDict, msg):
        values = []
        for value in self.old:
            if oldDict.get(value):
                values.append(oldDict[value])
        strg = self.sep.join(values)
        newDict[self.new] = strg
