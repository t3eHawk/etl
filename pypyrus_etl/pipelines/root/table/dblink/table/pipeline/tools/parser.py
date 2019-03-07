import pypyrus_etl as etl
import sqlalchemy as sql

from ..objects import Input, Medium, Output, ErrorHandler

class Parser():
    """
    That class represents the parser used to process some objects during
    the ETL process.
    """
    def __init__(self, pipeline):
        self.pipeline = pipeline
        pass

    def parse_input(self):
        pipeline = self.pipeline
        raw = self.pipeline.raw
        config = pipeline.config
        query = config.data.get('query')
        if isinstance(query, dict) is True:
            name = query.get('table') or raw.name
            schema = query.get('schema') or raw.schema

        object = Input(name, schema=schema, pipeline=pipeline)
        return object

    def parse_medium(self):
        pipeline = self.pipeline
        db = pipeline.target
        config = pipeline.config

        name = config.parse_table_name('raw')
        schema = db.schema

        object = Medium(name, schema=schema, database=db, pipeline=pipeline)
        return object

    def parse_output(self):
        pipeline = self.pipeline
        db = pipeline.target
        config = pipeline.config

        name = config.parse_table_name('ds')
        schema = db.schema

        object = Output(name, schema=schema, database=db, pipeline=pipeline)
        return object

    def parse_error_handler(self):
        pipeline = self.pipeline
        db = pipeline.target
        config = pipeline.config

        name = config.parse_table_name('eh')
        schema = db.schema

        object = ErrorHandler(
            name, schema=schema, database=db, pipeline=pipeline)
        return object
