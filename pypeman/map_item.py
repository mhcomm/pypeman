import time
import logging

logger = logging.getLogger(__name__)

class MapItem:
    def __init__(self, old, new=None, default=None):
        self.old = old
        self.new = new
        self.default = default

    def conv(self, oldDict, newDict, msg):
        if oldDict.get(self.old):
            newDict[self.old] = oldDict.pop(self.old)
        elif self.default is not None:
            newDict[self.old] = self.default


class RenameMapItem(MapItem):
    def conv(self, oldDict, newDict, msg):
        if oldDict.get(self.old):
            newDict[self.new] = oldDict.pop(self.old)
        elif self.default is not None:
            newDict[self.old] = self.default


class RenameSubMapItem(MapItem):
    def conv(self, oldDict, newDict, msg):
        value = oldDict
        i=1
        for key in self.old:
            if i == len(self.old):
                if value.get(key):
                    value = value.pop(key)
                else:
                    value = self.default

            else:
                i = i + 1
                if value.get(key):
                    value = value[key]
                else:
                    value = self.default
                    break

        cdict = newDict
        i=1
        for key in self.new:
            if i == len(self.new):
                cdict[key] = value
            else:
                if not cdict.get(key):
                    cdict[key] = {}
                cdict = cdict[key]
                i = i + 1


class MultipleRenameMapItem(MapItem):
    def conv(self, oldDict, newDict, msg):
        for value in old:
            if oldDict[value]:
                newDict[new] = oldDict.pop(value)
                return


class AddMapItem(MapItem):
    def __init__(self, new, value):
        self.value = value
        super().__init__(old=None, new=new)
    def conv(self, oldDict, newDict, msg):
        newDict[self.new] = self.value


class ConvDateMapItem(MapItem):
    def __init__(self, old, new, oldFormat, newFormat):
        self.oldFormat = oldFormat
        self.newFormat = newFormat
        super().__init__(old, new)
    def conv(self, oldDict, newDict, msg):
        val = time.strptime(oldDict.pop(self.old), self.oldFormat)
        val = time.strftime(self.newFormat, val)
        newDict[self.new] = val


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
