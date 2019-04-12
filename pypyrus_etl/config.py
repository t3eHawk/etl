import os
import json

class Configurator():
    def __new__(self, path=None):
        path = os.path.abspath(path)
        if os.path.exists(path) is True:
            with open(path, 'r') as fh:
                configurator = json.load(fh)
                return configurator
        else:
            return {}
