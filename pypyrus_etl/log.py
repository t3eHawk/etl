import pypyrus_logbook as logbook

class Logger():
    def __new__(self):
        return logbook.Logger()
