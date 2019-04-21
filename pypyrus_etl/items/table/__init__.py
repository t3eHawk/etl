import threading
import sqlalchemy as sql
import pypyrus_logbook as logbook

from ...log import make_log
from ...queue import queue
from ...config import make_config
from ...nodes.database import Database, make_database

class Table():
    """This class represents a table object with its description."""
    def __init__(
        self, database=None, name=None, schema=None, dblink=None,
        columns=None, primary_key=None, foreign_keys=None, compress=False,
        alias=None, joins=None, select_all=False, parallel=False, purge=False,
        fetch=1000, commit=1000, config=None
    ):
        # Basic logger.
        self.log = make_log()
        self.database = None
        self.db = None
        self.entity = None
        self.name = None
        self.schema = None
        self.dblink = None
        self.columns = []
        self.primary_key = None
        self.foreign_keys = []
        self.compress = None
        self.alias = None
        self.joins = []
        self.select_all = None
        self.parallel = None
        self.purge = None
        self.fetch = None
        self.commit = None
        self.count = 0

        self.configure(
            database=database, name=name, schema=schema, dblink=dblink,
            columns=columns, primary_key=primary_key, foreign_keys=foreign_keys,
            compress=compress, alias=alias, joins=joins, select_all=select_all,
            parallel=parallel, purge=purge, fetch=fetch, commit=commit,
            config=config)
        pass

    def configure(
        self, database=None, name=None, schema=None, dblink=None,
        columns=None, primary_key=None, foreign_keys=None, compress=False,
        alias=None, joins=None, select_all=None, parallel=None, purge=None,
        fetch=None, commit=None, config=None
    ):
        # Process config if exists.
        if config is not None:
            config = make_config(obj=config)
            database = config.get('database', database)
            name = config.get('name', name)
            schema = config.get('schema', schema)
            dblink = config.get('dblink', dblink)
            columns = config.get('columns', columns)
            primary_key = config.get('primary_key', primary_key)
            foreign_keys = config.get('foreign_keys', foreign_keys)
            compress = config.get('compress', compress)
            alias = config.get('alias', alias)
            joins = config.get('joins', joins)
            select_all = config.get('select_all', select_all)
            parallel = config.get('parallel', parallel)
            purge = config.get('purge', purge)
            fetch = config.get('fetch', dblink)
            commit = config.get('commit', commit)

        # Identification.
        if isinstance(name, str) is True:
            self.name = name.lower()
        if isinstance(schema, str) is True:
            self.schema = schema.lower()
        if isinstance(dblink, str) is True:
            self.dblink = dblink.lower()

        # Database which stores that table.
        if isinstance(database, Database) is True:
            self.database = self.db = database
        elif isinstance(database, str) is True:
            self.database = self.db = make_database(database)

        # The definition related stuff.
        if isinstance(columns, list) is True:
            self.columns = columns
        if isinstance(primary_key, (list, dict)) is True:
            self.primary_key = primary_key
        if isinstance(foreign_keys, list) is True:
            self.foreign_keys = foreign_keys
        if isinstance(compress, bool) is True:
            self.compress = compress

        # The query related stuff.
        if isinstance(alias, str) is True:
            self.alias = alias.lower()
        if isinstance(select_all, bool) is True:
            self.select_all = select_all
        if isinstance(joins, list) is True:
            self.joins = joins
        if isinstance(parallel, bool) is True:
            self.parallel = parallel
        if isinstance(purge, bool) is True:
            self.purge = purge
        if isinstance(fetch, int) is True:
            self.fetch = fetch
        if isinstance(commit, int) is True:
            self.commit = commit
        pass

    def __str__(self):
        string = self.name
        if isinstance(self.schema, str) is True:
            string = f'{self.schema}.{string}'
        if isinstance(self.dblink, str) is True:
            string = f'{string}@{self.dblink}'
        return string

    def create(self):
        columns = self._parse_columns()
        primary_key = self._parse_primary_key()
        foreign_keys = self._parse_foreign_keys()
        params = self._parse_params()

        self.entity = sql.Table(
            self.name, self.db.metadata,
            *columns, primary_key, *foreign_keys,
            **params, schema=self.schema)
        self.entity.create(self.db.engine, checkfirst=True)
        self.log.info(f'Table {self} creaetd')
        pass

    def load(self):
        self.entity = sql.Table(
            self.name, self.db.metadata,
            autoload=True, autoload_with=self.db.engine,
            schema=self.schema)
        self.log.debug(f'Table {self} loaded')
        pass

    def delete(self):
        query = self.entity.delete()
        self.db.connection.execute(query)
        pass

    def drop(self):
        self.entity.drop(self.db.engine)
        pass

    def insert(self, chunk):
        query = self.entity.insert()
        self.db.connection.execute(query, chunk)
        self.count += len(chunk)
        self.log.info(f'{self.count} records inserted.')
        pass

    def select(self):
        query = self._parse_query()
        self.log.debug(f'Query:\n{query}')
        self.result = self.db.connection.execute(query)
        self.log.info(f'Query {self.name} executed')
        return self.result

    def fetchone(self):
        chunk = self.result.fetchone()
        if chunk:
            self.count += 1
            self.log.info(f'{self.count} records fetched')
        return chunk

    def fetchmany(self):
        chunk = self.result.fetchmany(self.fetch)
        if chunk:
            self.count += len(chunk)
            self.log.info(f'{self.count} records fetched')
        return chunk

    def fetchall(self):
        chunk = self.result.fetchall()
        if chunk:
            self.count += len(chunk)
            self.log.info(f'{self.count} records fetched')
        return chunk

    def select_count(self):
        query = self._parse_select_count()
        self.log.debug(f'Query:\n{query}')
        result = self.db.connection.execute(query).scalar()
        self.result = self.count = result
        return result

    def insert_select(select):
        query = self.entity.insert().from_select(select.columns, select)
        self.db.connection.execute(query)
        pass

    def extract(self):
        self.select()
        self.log.info('Fetching records...')
        while True:
            chunk = self.fetchmany()
            self.log.set(input_count=self.count)
            if chunk:
                queue.put(chunk)
            else:
                break
        pass

    def load(self, threads=4):
        for i in range(threads):
            thread = threading.Thread(target=self._load, daemon=True)
            thread.start()
            self.log.debug(f'Thread {thread} started')
        pass

    @property
    def exists(self):
        if self.db is not None:
            return self.db.engine.has_table(self.name)

    @property
    def query(self):
        select = sql.text(self._parse_query())
        columns = [sql.column(column['name']) for column in self.columns]
        select = select.columns(*columns)
        return select

    def _parse_columns(self):
        self.log.debug(f'Parsing columns of {self}...')
        for column in self.columns:
            name = column['name']
            self.log.debug(f'Column {name}...')

            source = column.get('source', False)
            source = None if source is False else source

            table = column.get('table', self.name)
            schema = column.get('schema', self.schema)
            dblink = column.get('dblink', self.dblink)

            datatype = column.get('type')
            length = column.get('length')
            precision = column.get('precision')
            scale = column.get('scale')

            datatype = self._inspect_column_datatype(
                source, datatype, table, schema, dblink)
            length = self._inspect_column_length(
                source, length, table, schema, dblink)
            precision = self._inspect_column_precision(
                source, precision, table, schema, dblink)
            scale = self._inspect_column_scale(
                source, scale, table, schema, dblink)

            description = self._compile_column_description(
                datatype, length, precision, scale)

            colargs = []
            colkwargs = {}

            colargs.append(description)

            if column.get('sequence') is not None:
                sequence = column.get('sequence')
                if isinstance(sequence, str) is True:
                    sequence = sql.Sequence(sequence)
                elif sequence is True:
                    sequence = f'{self.name}_seq'
                    sequence = sql.Sequence(sequence)
                colargs.append(sequence)

            if column.get('foreign_key') is not None:
                foreign_key = column['foreign_key']
                foreign_key = sql.ForeignKey(foreign_key)
                colargs.append(foreign_key)

            attributes = [
                'autoincrement', 'default', 'index', 'nullable',
                'primary_key', 'unique', 'comment']
            for attribute in attributes:
                if column.get(attribute) is not None:
                    colkwargs[attribute] = column[attribute]

            self.log.debug('done')
            yield sql.Column(name, *colargs, **colkwargs)

    def _parse_primary_key(self):
        if isinstance(self.primary_key, list) is True:
            return sql.PrimaryKeyConstraint(*self.primary_key)
        elif isinstance(self.primary_key, dict) is True:
            name = self.primary_key.get('name')
            keys = self.primary_key.get('keys', [])
            return sql.PrimaryKeyConstraint(*keys, name=name)

    def _parse_foreign_keys(self):
        for key in self.foreign_keys:
            name = key.get('name')
            table = key.get('table')
            column = key.get('column')
            refcolumn = key.get('refcolumn', column)
            columns = key.get('columns')
            refcolumns = key.get('refcolumns', columns)

            if isinstance(column, str) is True:
                if isinstance(refcolumn, str) is True:
                    refcolumn = f'{table}.{refcolumn}'
                    yield sql.ForeignKeyConstraint(
                        [column], [refcolumn], name=name)
            elif isinstance(columns, list) is True:
                if isinstance(refcolumns, list) is True:
                    refcolumns = [
                        f'{table}.{refcolumn}'
                        for refcolumn in refcolumns]
                    yield sql.ForeignKeyConstraint(
                        columns, refcolumns, name=name)

    def _parse_params(self):
        params = {}
        vendor = self.db.vendor
        if vendor == 'oracle':
            params['oracle_compress'] = self.compress
        return params

    def _parse_query(self):
        query = []

        select = self._parse_select()
        from_ = self._parse_from()
        join = self._parse_join()
        where = self._parse_where()

        for statement in [select, from_, join, where]:
            if len(statement) > 0:
                query.append(statement)

        query = '\n'.join(query)
        query = query.format(**self.log.sysinfo.prms)
        return query

    def _parse_select(self):
        pointer = self.alias or self.name

        statement = [r'SELECT']
        # Enable parallel execution for Oracle.
        if hasattr(self.db, 'vendor') is True:
            if self.db.vendor == 'oracle':
                hint = self._parse_oracle_parallel()
                if hint is not None:
                    statement.append(hint)

        # Select all original fields or define the list of necessary fields.
        if self.select_all is True:
            statement.append(f'{pointer}.*')
        elif self.select_all is False:
            fields = []
            for column in self.columns:
                # Skip new fields because they are not in input.
                select = column.get('select', True)
                sql = column.get('sql')
                if isinstance(sql, str) is True:
                    fields.append(sql)
                elif select is True:
                    name = column['name']
                    table = column.get('table', pointer)
                    value = column.get('value')
                    field = value or f'{table}.{name}'.lower()

                    trim = column.get('trim', False)
                    to_char = column.get('to_char', False)
                    if trim is True or to_char is True or value is not None:
                        if trim is True:
                            field = f'TRIM({field})'
                        if to_char is True:
                            field = f'TO_CHAR({field})'
                        field = f'{field} AS {name}'

                    fields.append(field)
            fields = ', '.join(fields)
            statement.append(fields)
        statement = ' '.join(statement)
        return statement

    def _parse_from(self):
        statement = ['FROM']

        table = self.name
        if isinstance(self.schema, str) is True:
            table = f'{self.schema}.{table}'
        if isinstance(self.dblink, str) is True:
            table = f'{table}@{self.dblink}'
        statement.append(table)
        if isinstance(self.alias, str) is True:
            statement.append(self.alias)

        statement = ' '.join(statement)
        return statement

    def _parse_join(self):
        statements = []
        for join in self.joins:
            statement = []

            table = join['table']
            schema = join.get('schema', self.schema)
            dblink = join.get('dblink')
            alias = join.get('alias')
            pointer = alias or table

            type = join.get('type', 'inner').upper()
            statement.append(f'{type} JOIN')
            jointable = table
            if isinstance(schema, str) is True:
                jointable = f'{schema}.{jointable}'
            if isinstance(dblink, str) is True:
                jointable = f'{jointable}@{dblink}'
            statement.append(jointable)

            statement.append(f'\nON')
            basepointer = self.alias or self.name
            key = join.get('key')
            operator = join.get('operator', '=')
            if isinstance(key, str) is True:
                statement.append(f'{basepointer}.{key}')
                statement.append(operator)
                statement.append(f'{pointer}.{key}')

            keys = join.get('keys')
            if isinstance(keys, list) is True:
                for i, key in enumerate(keys):
                    k_key = key['key']
                    k_table = key.get('table', basepointer)
                    k_column = key.get('column', k_key)
                    k_connector = key.get('connector', 'AND').upper()
                    k_operator = key.get('operator', '=').upper()

                    if i > 0:
                        statement.append(f'\n{k_connector}')
                    statement.append(f'{k_table}.{k_column}')
                    statement.append(k_operator)
                    statement.append(f'{pointer}.{k_key}')

            statement = ' '.join(statement)
            statements.append(statement)

        statements = '\n'.join(statements)
        return statements

    def _parse_where(self):
        statements = []
        for column in self.columns:
            name = column['name']
            table = column.get('table', self.alias or self.name)
            field = f'{table}.{name}'
            filters = column.get('filters', [])
            for filter in filters:
                statement = []

                type = filter.get('type', 'and').upper()
                operator = filter.get('operator', '=').upper()
                value = filter.get('value')

                values = [value] if isinstance(value, list) is False else value

                if len(statements) == 0:
                    if 'NOT' in type:
                        statement.append(f'WHERE NOT {field}')
                    else:
                        statement.append(f'WHERE {field}')
                else:
                    statement.append(f'{type} {field}')

                values = list(map(self._map_value, values))

                if 'NULL' in values:
                    operator = 'IS'
                statement.append(operator)

                count_values = len(values)
                if count_values == 1:
                    statement.append(values[0])
                elif operator == 'BETWEEN' and count_values == 2:
                    statement.append(f'{value[0]} AND {value[1]}')
                else:
                    values = ', '.join(values)
                    statement.append(f'({values})')

                statement = ' '.join(statement)
                statements.append(statement)

        statements = '\n'.join(statements)
        return statements


    def _inspect_column_datatype(
        self, name, datatype=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and datatype is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_type'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': name.upper()}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': schema.upper()}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': table.upper()}],
                        'select': False}
                    config['columns'].append(column)

                select = Table(**config)._parse_query()
                datatype = db.connection.execute(select).scalar()

        # Choose correct SQLAlchemy datatype class name.
        datatype = datatype.upper()
        if datatype in ['NUMBER', 'DECIMAL', 'NUMERIC']:
            datatype = 'Numeric'
        elif datatype in ['INTEGER', 'INT']:
            datatype = 'Integer'
        elif datatype in ['FLOAT', 'REAL', 'DOUBLE']:
            datatype = 'Float'
        elif datatype in ['STRING', 'TEXT', 'VARCHAR', 'VARCHAR2', 'NVARCHAR']:
            datatype = 'String'
        elif datatype in ['DATE']:
            datatype = 'Date'
        elif datatype in ['DATETIME', 'TIMESTAMP']:
            datatype = 'DateTime'
        elif datatype in ['TIME']:
            datatype = 'Time'
        elif datatype in ['BOOL', 'BOOLEAN']:
            datatype = 'Boolean'

        datatype = getattr(sql, datatype)
        return datatype

    def _inspect_column_length(
        self, name, length=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and length is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_length'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': name.upper()}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': schema.upper()}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': table.upper()}],
                        'select': False}
                    config['columns'].append(column)

                    select = Table(**config)._parse_query()
                    length = db.connection.execute(select).scalar()

        return length

    def _inspect_column_precision(
        self, name, precision=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and precision is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_precision'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': name.upper()}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': schema.upper()}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': table.upper()}],
                        'select': False}
                    config['columns'].append(column)

                select = Table(**config)._parse_query()
                precision = db.connection.execute(select).scalar()

        return precision

    def _inspect_column_scale(
        self, name, scale=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and scale is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_scale'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': name.upper()}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': schema}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': table.upper()}],
                        'select': False}
                    config['columns'].append(column)

                select = Table(**config)._parse_query()
                scale = db.connection.execute(select).scalar()

        return scale

    def _compile_column_description(self, datatype, length, precision, scale):
        if datatype is sql.Numeric:
            return datatype(precision, scale)
        elif datatype is sql.Integer:
            return datatype
        elif datatype is sql.Float:
            return datatype(precision)
        elif datatype is sql.String:
            return datatype(length)
        elif datatype is sql.Date:
            return datatype
        elif datatype is sql.DateTime:
            return datatype
        elif datatype is sql.Time:
            return datatype
        elif datatype is sql.Boolean:
            return datatype

    def _map_value(self, value):
        if value is None:
            return 'NULL'
        elif isinstance(value, str) is True:
            return f"'{value}'"
        else:
            return str(value)

    def _parse_oracle_parallel(self):
        if self.parallel is True:
            return '/*+ PARALLEL(auto) */'
        elif isinstance(self.parallel, int)  is True:
            return f'/* PARALLEL({self.parallel}) */'

    def _load(self):
        while True:
            chunk = queue.get()
            try:
                self.insert(chunk)
            except sql.exc.SQLAlchemyError:
                self._insert_with_error(chunk)
            finally:
                self.log.set(output_count=self.count)
                queue.task_done()
        pass

    def _insert_with_error(self, chunk):
        query = self.entity.insert()
        for row in chunk:
            try:
                current_query = query.values(**row)
                self.db.connection.execute(current_query)
            except sql.exc.SQLAlchemyError:
                pass
            else:
                self.count += 1
        self.log.info(f'{self.count} records inserted.')
        pass

    def _parse_count(self):
        statement = [r'SELECT']
        if hasattr(self.db, 'vendor') is True:
            if self.db.vendor == 'oracle':
                hint = self._parse_oracle_parallel()
                if hint is not None:
                    statement.append(hint)
        statement.append('COUNT(*)')
        statement = ' '.join(statement)
        return statement

    def _parse_select_count(self):
        query = []

        select = self._parse_count()
        from_ = self._parse_from()
        join = self._parse_join()
        where = self._parse_where()

        for statement in [select, from_, join, where]:
            if len(statement) > 0:
                query.append(statement)

        query = '\n'.join(query)
        query = query.format(**self.log.sysinfo.prms)
        return query
