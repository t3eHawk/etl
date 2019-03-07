import os
import json

class Config():
    def __init__(self, pipeline, object=None):
        """
        That class represents the configurator for an ETL process for objects
        loaded from the source to the target objectbase using dblink.
        """
        self._pipeline = pipeline
        self._data = self._parse_configurator(object)
        pass

    def __getitem__(self, key):
        return self._data.get(key)

    def _parse_configurator(self, object):
        """
        Parse object and transform it to the desc attribute according to its
        format whether it is a ready dictionary or a path to the JSON file.
        """
        log = self._pipeline.log
        log.sys.subhead('configuration')
        try:
            log.sys.info('Parsing configurator.')
            if object is None:
                log.sys.warning('Configurator is empty!')
                configurator = {}
            elif isinstance(object, str) is True:
                if os.path.exists(object) is True:
                    log.sys.info('Configurator is JSON file.')
                    path = os.path.abspath(object)
                    with open(path, 'r') as fh:
                        configurator = json.load(fh)
            elif isinstance(object, dict) is True:
                log.sys.info('Configurator is dictionary.')
                configurator = object
        except:
            log.sys.critical()
        else:
            log.sys.info('Configurator parsed.')
            return configurator
