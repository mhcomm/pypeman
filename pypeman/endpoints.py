from pypeman.helpers import lazyload

all = []

# used to share external dependencies
ext = {}

class BaseEndpoint:
    dependencies = [] # List of module requirements

    def __init__(self):
        all.append(self)

    def requirements(self):
        """ List dependencies of modules if any """
        return self.dependencies

    def import_modules(self):
        """ Use this method to import specific external modules listed in dependencies """
        pass


HTTPEndpoint = lazyload.load(__name__, 'pypeman.contrib.http', 'HTTPEndpoint', ['aiohttp'])
MLLPEndpoint = lazyload.load(__name__, 'pypeman.contrib.hl7', 'MLLPEndpoint', ['hl7'])

