import sqlalchemy as sql

from pypyrus_etl.objects.table import Table

class Medium(Table):
    """Represents output object of that ETL pipeline."""

    def load(self):
        db = self.pipeline.target
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
        db = self.pipeline.target
        tbname = self.name

        log = self.pipeline.log
        config = self.pipeline.config

        # Create empty table with the same structure as in the source.
        log.sys.info(f'Create table <{db.name}.{self.schema}.{self.name}>.')

        create = [f'CREATE TABLE {tbname} AS']
        select = config.parse_select()
        from_ = config.parse_from()
        join = config.parse_join()
        for block in [select, from_, join]:
            if block is not None:
                create.append(block)
        create.append('WHERE 1 = 0')

        create = '\n'.join(create)
        log.sys.info(f'With query:\n\n{create}\n')

        db.connection.execute(create)
        table = sql.Table(
            tbname, db.metadata,
            autoload=True, autoload_with=db.engine)

        self.data = table
        log.sys.info('Table created.')
        pass

    def drop(self):
        db = self.pipeline.target
        tbname = self.name

        log = self.pipeline.log

        log.sys.info(f'Drop table <{db.name}.{self.schema}.{self.name}>.')

        drop = self.data.drop(db.engine)
        log.sys.info('Table dropped.')
        pass

    def insert(self):
        db = self.pipeline.target
        tbname = self.name

        source = self.pipeline.source
        input = self.pipeline.input
        target = self.pipeline.target

        log = self.pipeline.log
        config = self.pipeline.config

        insert = f'INSERT INTO {tbname}\n'
        insert += config.parse_query()

        log.sys.info(
            'Load data '\
            f'from <{source.name}.{input.schema}.{input.name}> '\
            f'to <{target.name}.{self.schema}.{self.name}>.')
        log.sys.info(f'With query:\n\n{insert}\n')

        db.connection.execute(insert)
        log.sys.info('Insert completed.')
        pass

    def prepare(self):
        db = self.pipeline.target
        tbname = self.name

        if db.engine.has_table(tbname) is True:
            self.load()
            self.drop()
        self.create()
        pass

    def process_duplicates(self):
        pipeline = self.pipeline

        db = pipeline.target
        output = pipeline.output

        log = pipeline.log
        config = pipeline.config
        parser = pipeline.parser

        log.sys.info(
            'Process record duplicates '\
            f'between <{db.name}.{self.schema}.{self.name}> '\
            f'and <{db.name}.{self.schema}.{self.name}>.')

        # Names of columns that are loaded from input table to output table.
        columns = config.data['columns']

        names_source = []
        for column in columns:
            if column.get('new', False) is False:
                if column.get('load', True) is True:
                    name = column['name']
                    names_source.append(name)

        names_target = []
        for column in columns:
            if column.get('new', False) is False:
                if column.get('load', True) is True:
                    name = column.get('rename') or column['name']
                    names_target.append(name)

        # Needed columns from medium table that stored in output table.
        columns_medium = [self.data.c[column] for column in names_source]
        columns_medium = list(columns_medium)

        # Needed columns from output table.
        columns_output = [output.data.c[column] for column in names_target]
        columns_output = list(columns_output)

        # Select of needed columns from output table.
        select_output = sql.select(columns_output, distinct=True)
        load_id = sql.literal(log.load_id).label('load_id')
        # Error type.
        error_type = sql.literal('duplicate').label('error_type')

        # Select of records from medium table that are duplicates to records
        # already stored in output table.
        select = sql.select([self.data, load_id, error_type]).\
            where(sql.tuple_(*columns_medium).in_(select_output))

        # Count duplicates number.
        count = sql.select([sql.func.count()]).\
            select_from(select)
        result = db.connection.execute(count).scalar()

        # Error hander will be created or loaded only if duplicates appeared.
        log.sys.info(f'Found duplicates: <{result}>.')
        if result > 0:
            # Move duplicate records from medium table to error handler.
            eh = pipeline.error_handler = parser.parse_error_handler()
            eh.prepare()

            insert = eh.data.insert().from_select(eh.data.c.keys(), select)

            delete = self.data.delete().\
                where(sql.tuple_(*columns_medium).in_(select_output))

            db.connection.execute(insert)
            db.connection.execute(delete)

            log.sys.info(
                f'Duplicates moved to <{db.name}.{eh.schema}.{eh.name}>.')
            pipeline.with_error = True
        pass

    def process_primary_key_duplicates(self):
        pipeline = self.pipeline

        db = pipeline.target
        output = pipeline.output

        log = pipeline.log
        config = pipeline.config
        parser = pipeline.parser

        log.sys.info(
            'Process primary key duplicates '\
            f'between <{db.name}.{self.schema}.{self.name}> '\
            f'and <{db.name}.{output.schema}.{output.name}>.')

        names_source = []
        for key in output.data.primary_key:
            column = config.get_column(name=key.name, original=False)
            name = column['name']
            names_source.append(name)

        columns_output = output.data.primary_key

        columns_medium = [self.data.c[column] for column in names_source]
        columns_medium = list(columns_medium)

        select_output = sql.select(columns_output, distinct=True)
        load_id = sql.literal(log.load_id).label('load_id')
        # Error type.
        error_type = sql.literal('pk_error').label('error_type')

        select = sql.select([self.data, load_id, error_type]).\
            where(sql.tuple_(*columns_medium).in_(select_output))

        # Count duplicates number.
        count = sql.select([sql.func.count()]).select_from(select)
        result = db.connection.execute(count).scalar()

        # Error hander will be created or loaded only if duplicates appeared.
        log.sys.info(f'Found primary key duplicates: <{result}>.')
        if result > 0:
            # Move duplicate records from medium table to error handler.
            eh = pipeline.error_handler = parser.parse_error_handler()
            eh.prepare()

            insert = eh.data.insert().from_select(eh.data.c.keys(), select)

            delete = self.data.delete().\
                where(sql.tuple_(*columns_medium).in_(select_output))

            db.connection.execute(insert)
            db.connection.execute(delete)

            log.sys.info(
                'Primary key duplicates moved to '\
                f'{db.name}.{eh.schema}.{eh.name}.')
            pipeline.with_error = True
        pass
