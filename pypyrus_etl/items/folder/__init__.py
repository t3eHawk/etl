from pypyrus_etl.objects import Base

class Folder(Base):
    def __init__(self, name, path=None, pipeline=None):
        super().__init__(name, pipeline=pipeline)
        # Exclude last / if it was passed.
        if path is not None:
            path = path[:-1] if path[-1] == '/' else path
        self.path = path
        pass
