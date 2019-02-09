from pypyrus_etl.objects import Base

class Table(Base):
    """This class represents a table object with its description."""
    def __init__(
        self, name, data=None, schema=None, database=None, pipeline=None
    ):
        super().__init__(name, pipeline=pipeline)
        self.data = data
        self.schema = None if schema is None else schema.lower()
        self.database = database
        pass
