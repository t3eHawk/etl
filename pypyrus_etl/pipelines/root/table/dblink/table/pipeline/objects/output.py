import pypyrus_etl as etl
import sqlalchemy as sql

from pypyrus_etl.objects.table import Table

class Output(Table):
    """Represents output object of that ETL pipeline."""

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

        log = self.pipeline.log
        config = self.pipeline.config

        log.sys.info(f'Create table <{db.name}.{self.schema}.{self.name}>.')

        columns = config.parse_columns()
        primary_key = config.parse_primary_key()
        foreign_keys = config.parse_foreign_keys()
        params = config.parse_params()

        table = sql.Table(
            tbname, db.metadata,
            *columns, primary_key, *foreign_keys, **params)
        table.create(db.engine, checkfirst=True)

        self.data = table
        log.sys.info('Table created.')
        pass

    def delete(self):
        db = self.database

        pipeline = self.pipeline
        log = pipeline.log
        config = pipeline.config

        parallel = config.data.get('parallel', False)
        dop = config.data.get('dop', 'auto')

        log.sys.info(f'Delete all in <{db.name}.{self.schema}.{self.name}>.')

        delete = self.data.delete()
        if parallel is True:
            delete = etl.database.dml.parallel(delete, dop=dop)

        stmt = delete.compile(**db.compargs)
        log.sys.info(f'With query:\n\n{stmt}\n')

        db.connection.execute(delete)
        log.sys.info('Data in table deleted.')
        pass

    def insert(self):
        db = self.database

        pipeline = self.pipeline
        medium = pipeline.medium
        log = pipeline.log
        config = pipeline.config

        parallel = config.data.get('parallel', False)
        dop = config.data.get('dop', 'auto')

        log.sys.info(
            'Load data '\
            f'from <{db.name}.{medium.schema}.{medium.name}> '\
            f'to <{db.name}.{self.schema}.{self.name}>.')

        output_columns = self.get_columns(only_names=True, insert=True)
        medium_columns = config.pick_medium_columns()
        load_id = sql.literal(log.load_id).label('load_id')

        select = sql.select([*medium_columns, load_id])
        insert = self.data.insert().\
            from_select([*output_columns, 'load_id'], select)
        if parallel is True:
            insert = etl.database.dml.parallel(insert, dop=dop)

        stmt = insert.compile(**db.compargs)
        log.sys.info(f'With query:\n\n{stmt}\n')

        db.connection.execute(insert)
        log.sys.info('Insert completed.')
        pass

    def update(self):
        db = self.database

        pipeline = self.pipeline
        medium = pipeline.medium
        log = pipeline.log
        config = pipeline.config

        parallel = config.data.get('parallel', False)
        dop = config.data.get('dop', 'auto')

        log.sys.info(
            'Update data '\
            f'from <{db.name}.{medium.schema}.{medium.name}> '\
            f'in <{db.name}.{self.schema}.{self.name}>.')

        load_id = sql.literal(log.load_id).label('load_id')
        update_id = sql.literal(log.load_id).label('update_id')

        table = self.data
        using = sql.select([medium.data, load_id, update_id])
        keys = config.data['update']['keys']
        columns = config.data['update']['columns']
        columns.append('update_id')

        update = etl.database.dml.update(table, using, keys, columns)
        if parallel is True:
            update = etl.database.dml.parallel(update, dop=dop)

        stmt = update.compile(**db.compargs)
        log.sys.info(f'With query:\n\n{stmt}\n')

        db.connection.execute(update)
        log.sys.info('Update completed.')
        pipeline.with_update = True
        pass

    def merge(self):
        db = self.database

        pipeline = self.pipeline
        medium = pipeline.medium
        log = pipeline.log
        config = pipeline.config

        parallel = config.data.get('parallel', False)
        dop = config.data.get('dop', 'auto')

        log.sys.info(
            'Merge data '\
            f'from <{db.name}.{medium.schema}.{medium.name}> '\
            f'to <{db.name}.{self.schema}.{self.name}>.')

        load_id = sql.literal(log.load_id).label('load_id')
        update_id = sql.literal(log.load_id).label('update_id')

        table = self.data
        using = sql.select([medium.data, load_id, update_id])
        keys = config.data['merge']['keys']
        updcols = config.data['merge']['columns']
        updcols.append('update_id')
        inscols = self.get_columns(only_names=True, insert=True, pair=True)
        inscols = [*inscols, 'load_id']

        merge = etl.database.dml.merge(table, using, keys, updcols, inscols)
        if parallel is True:
            merge = etl.database.dml.parallel(merge, dop=dop)

        stmt = merge.compile(**db.compargs)
        log.sys.info(f'With query:\n\n{stmt}\n')

        db.connection.execute(merge)
        log.sys.info('Merge completed.')
        pipeline.with_update = True
        pass

    def prepare(self):
        db = self.database
        tbname = self.name

        if db.engine.has_table(tbname) is True:
            self.load()
        else:
            self.create()
        pass

    def get_columns(self, only_names=False, insert=False, pair=False):
        config = self.pipeline.config
        table = self.data
        for column in table.columns:
            params = config.get_column(name=column.name, original=False)
            load = params.get('load', True)
            new = params.get('new', False)
            if insert is True:
                if load is True and new is False:
                    if only_names is True:
                        name = params['name']
                        rename = params.get('rename')
                        if pair is True and rename is not None:
                            yield [column.name, name]
                        else:
                            yield column.name
                    elif only_names is False:
                        yield column
            elif insert is False:
                if only_names is True:
                    yield column.name
                elif only_names is False:
                    yield column
