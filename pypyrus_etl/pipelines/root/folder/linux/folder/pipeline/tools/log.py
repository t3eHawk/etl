import pypyrus_logbook as logbook

class Log():
    def __init__(self, pipeline, sys=None):
        self.pipeline = pipeline
        self.sys = sys or logbook.Log('pipeline')
        pass
