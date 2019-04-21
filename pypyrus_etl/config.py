import os
import json
import pypyrus_logbook as logbook

def make_config(obj=None):
    log = logbook.Logger()
    try:
        if isinstance(obj, str) is True:
            path = os.path.abspath(obj)
            if os.path.exists(path) is True:
                with open(path, 'r') as fh:
                    config = json.load(fh)
        elif isinstance(obj, dict) is True:
            config = obj
        else:
            log.warning('Configurator was not found!')
            config = {}
    except:
        log.critical()
    else:
        if len(config.keys()) > 0:
            if isinstance(obj, str) is True:
                log.debug(f'Configurator <{obj}> parsed')
            elif isinstance(obj, dict) is True:
                log.debug(f'Configurator {list(obj.keys())} parsed')
        else:
            log.warning('Configurator is empty!')
    finally:
        return config
