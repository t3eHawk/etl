import sqlalchemy as sql

class Table():
    """This class represents a table object with its description."""
    def __init__(
        self, name=None, schema=None, dblink=None, database=None,
        columns=None, primary_key=None, foreign_keys=None, compress=False,
        alias=None, joins=None, select_all=False, parallel=False
    ):
        # Identification.
        self.name = None if name is None else name.lower()
        self.schema = None if schema is None else schema.lower()
        self.dblink = None if dblink is None else dblink.lower()

        # Database which stores that table.
        self.database = database
        self.entity = None

        # The definition related stuff.
        self.columns = columns or []
        self.primary_key = primary_key
        self.foreign_keys = foreign_keys or []
        self.compress = compress

        # The query related stuff.
        self.alias = None if alias is None else alias.lower()
        self.select_all = select_all
        self.joins = joins or []
        self.parallel = parallel
        pass

    def create(self):
        columns = self.parse_columns()
        primary_key = self.parse_primary_key()
        foreign_keys = self.parse_foreign_keys()
        params = self.parse_params()

        self.entity = sql.Table(
            self.name, self.database.metadata,
            *columns, primary_key, *foreign_keys, **params)
        self.entity.create(self.database.engine, checkfirst=True)
        pass

    def load(self):
        self.entity = sql.Table(
            self.name, self.database.metadata,
            autoload=True, autoload_with=db.engine)
        pass

    def delete(self):
        pass

    def drop(self):
        pass

    def insert(self):
        pass

    def select(self):
        return self.parse_query()

    def parse_columns(self):
        for column in self.columns:
            name = column['name']
            input = column.get('input', name)

            table = column.get('table', self.name)
            schema = column.get('schema', self.schema)
            dblink = column.get('dblink', self.dblink)

            datatype = column.get('type')
            datatype = self._parse_column_datatype(
                input, datatype, table, schema, dblink)

            length = column.get('length')
            length = self._parse_column_length(
                input, length, table, schema, dblink)

            precision = column.get('precision')
            precision = self._parse_column_precision(
                input, precision, table, schema, dblink)

            scale = column.get('scale')
            scale = self._parse_column_scale(
                input, scale, table, schema, dblink)

            description = self._compile_column_description(
                datatype, length, precision, scale)

            colargs = []
            colkwargs = {}

            colargs.append(description)
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

            yield sql.Column(name, *colargs, **colkwargs)

    def parse_primary_key(self):
        if isinstance(self.primary_key, list) is True:
            return sql.PrimaryKeyConstraint(*self.primary_key)
        elif isinstance(self.primary_key, dict) is True:
            name = self.primary_key.get('name')
            keys = self.primary_key.get('keys', [])
            return sql.PrimaryKeyConstraint(*keys, name=name)

    def parse_foreign_keys(self):
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

    def parse_params(self):
        params = {}
        vendor = self.database.vendor
        if vendor == 'oracle':
            params['oracle_compress'] = self.compress
        return params

    def parse_query(self):
        query = []

        select = self.parse_select()
        from_ = self.parse_from()
        join = self.parse_join()
        where = self.parse_where()

        for statement in [select, from_, join, where]:
            if len(statement) > 0:
                query.append(statement)

        query = '\n'.join(query)
        return query

    def parse_select(self):
        pointer = self.alias or self.name

        statement = [r'SELECT']
        # Enable parallel execution for Oracle.
        if hasattr(self.database, 'vendor') is True:
            if self.database.vendor == 'oracle':
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
                if select is True:
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

    def parse_from(self):
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

    def parse_join(self):
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

    def parse_where(self):
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


    def _parse_column_datatype(
        self, name, datatype=None, table=None, schema=None, dblink=None
    ):
        db = self.database
        if datatype is None:
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

                select = Table(**config).parse_query()
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

    def _parse_column_length(
        self, name, length=None, table=None, schema=None, dblink=None
    ):
        db = self.database
        if length is None:
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

                    select = Table(**config).parse_query()
                    length = db.connection.execute(select).scalar()

        return length

    def _parse_column_precision(
        self, name, precision=None, table=None, schema=None, dblink=None
    ):
        db = self.database
        if precision is None:
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

                select = Table(**config).parse_query()
                precision = db.connection.execute(select).scalar()

        return precision

    def _parse_column_scale(
        self, name, scale=None, table=None, schema=None, dblink=None
    ):
        db = self.database
        if scale is None:
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

                select = Table(**config).parse_query()
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

    @property
    def exists(self):
        if self.database is not None:
            return self.database.engine.has_table(self.name)

    @property
    def query(self):
        return self.parse_query()
