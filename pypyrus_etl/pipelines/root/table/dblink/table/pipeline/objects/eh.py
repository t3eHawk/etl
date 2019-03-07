import sqlalchemy as sql

from pypyrus_etl.objects.table import Table

class ErrorHandler(Table):
    """Represents error handler object of that ETL pipeline."""

    def load(self):
        db = self.database
        tbname = self.name

        log = self.pipeline.log

        log.sys.info(f'Load table <{db.name}.{self.schema}.{self.name}>.')

        table = sql.Table(
            tbname, db.metadata,
            autoload=True, autoload_with=db.engine)

        self.data = table
        log.sys.info('Table loaded.')
        pass

    def create(self):
        db = self.database
        tbname = self.name
        medium = self.pipeline.medium

        log = self.pipeline.log

        log.sys.info(f'Create table <{db.name}.{self.schema}.{self.name}>.')

        create = [f'CREATE TABLE {tbname}']
        if db.vendor == 'oracle':
            create.append('COMPRESS')
        create.append(
            f'AS SELECT {medium.name}.*, '\
            '99999 as load_id, \'xxxxxxxxxx\' AS error_type')
        create.append(f'FROM {medium.name}')
        create.append('WHERE 1 = 0')
        create = '\n'.join(create)

        db.connection.execute(create)
        table = sql.Table(
            tbname, db.metadata,
            autoload=True, autoload_with=db.engine)

        self.data = table
        log.sys.info('Table created.')
        pass

    def prepare(self):
        db = self.database
        tbname = self.name
        if db.engine.has_table(tbname) is False:
            self.create()
        else:
            self.load()
        pass
