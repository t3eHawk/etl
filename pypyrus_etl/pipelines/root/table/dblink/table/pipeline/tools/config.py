import os
import re
import json
import configparser
import sqlalchemy as sql

from datetime import date, datetime, timedelta, timezone

class Config():
    """
    That class represents the configurator for an ETL process for objects
    loaded from the source to the target database using dblink.
    """
    def __init__(self, pipeline, object=None):
        self.pipeline = pipeline
        self.data = self.parse_configurator(object)
        pass

    def parse_configurator(self, object):
        """
        Parse object and transform it to the desc attribute according to its
        format whether it is a ready dictionary or a path to the JSON file.
        """
        log = self.pipeline.log
        log.sys.subhead('configuration')
        try:
            log.sys.info('Parsing configurator.')
            if object is None:
                log.sys.warning('Configurator is empty!')
                configurator = {}
            elif isinstance(object, str) is True:
                if os.path.exists(object) is True:
                    log.sys.info('Configurator is JSON file.')
                    path = os.path.abspath(object)
                    with open(path, 'r') as file:
                        configurator = json.load(file)
            elif isinstance(object, dict) is True:
                log.sys.info('Configurator is dictionary.')
                configurator = object
        except:
            log.sys.critical()
        else:
            log.sys.info('Configurator parsed.')
            return configurator

    def parse_columns(self):
        """
        Parse the columns description from the configuration data and transform
        it to the sqlalchemy column definition expression.
        """
        query = self.data.get('query')
        columns = self.data.get('columns')
        for column in columns:
            load = column.get('load', True)
            if load is True:
                table = column.get('table', query.get('table'))
                schema = column.get('schema', query.get('schema'))
                link = column.get('link', query.get('link'))

                colargs = []

                name = column['name']
                rename = column.get('rename')
                target_column_name = rename or name
                colargs.append(target_column_name)

                datatype = column.get('type')
                datatype = self.parse_column_datatype(
                    name, datatype, table, schema, link)

                length = column.get('length')
                length = self.parse_column_length(
                    name, length, table, schema, link)

                precision = column.get('precision')
                precision = self.parse_column_precision(
                    name, precision, table, schema, link)

                scale = column.get('scale')
                scale = self.parse_column_scale(
                    name, scale, table, schema, link)

                datatype = self.compile_column_datatype(
                    datatype, length, precision, scale)

                colargs.append(datatype)
                if column.get('foreign_key') is not None:
                    foreign_key = column['foreign_key']
                    foreign_key = sql.ForeignKey(foreign_key)
                    colargs.append(foreign_key)

                colkwargs = {}
                if column.get('autoincrement') is not None:
                    autoincrement = column['autoincrement']
                    colkwargs['autoincrement'] = autoincrement
                if column.get('default') is not None:
                    default = column['default']
                    colkwargs['default'] = default
                if column.get('index') is not None:
                    index = column['index']
                    colkwargs['index'] = index
                if column.get('nullable') is not None:
                    nullable = column['nullable']
                    colkwargs['nullable'] = nullable
                if column.get('primary_key') is not None:
                    primary_key = column['primary_key']
                    colkwargs['primary_key'] = primary_key
                if column.get('unique') is not None:
                    unique = column['unique']
                    colkwargs['unique'] = unique
                if column.get('comment') is not None:
                    comment = column['comment']
                    colkwargs['comment'] = comment

                yield sql.Column(*colargs, **colkwargs)

    def parse_column_datatype(self, name, datatype, table, schema, link):
        """
        Define column datatype according to configuration or and DB information.
        """
        database = self.pipeline.target

        if datatype is None:
            if database.vendor == 'oracle':
                table = table.upper()
                schema = schema.upper()
                name = name.upper()

                code = [
                    f'SELECT data_type FROM all_tab_columns@{link}',
                    f'WHERE owner = \'{schema}\'',
                    f'AND table_name = \'{table}\'',
                    f'AND column_name = \'{name}\'']
                code = '\n'.join(code)

                datatype = database.connection.execute(code).scalar()

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

    def parse_column_length(self, name, length, table, schema, link):
        """
        Define column length according to configuration or and DB information.
        """
        database = self.pipeline.target

        if length is None:
            if database.vendor == 'oracle':
                table = table.upper()
                schema = schema.upper()
                name = name.upper()

                code = [
                    f'SELECT data_length FROM all_tab_columns@{link}',
                    f'WHERE owner = \'{schema}\'',
                    f'AND table_name = \'{table}\'',
                    f'AND column_name = \'{name}\'']
                code = '\n'.join(code)

                length = database.connection.execute(code).scalar()

        return length

    def parse_column_precision(self, name, precision, table, schema, link):
        """
        Define column precision according to configuration or and DB
        information.
        """
        database = self.pipeline.target

        if precision is None:
            if database.vendor == 'oracle':
                table = table.upper()
                schema = schema.upper()
                name = name.upper()

                code = [
                    f'SELECT data_precision FROM all_tab_columns@{link}',
                    f'WHERE owner = \'{schema}\'',
                    f'AND table_name = \'{table}\'',
                    f'AND column_name = \'{name}\'']
                code = '\n'.join(code)

                precision = database.connection.execute(code).scalar()

        return precision

    def parse_column_scale(self, name, scale, table, schema, link):
        """
        Define column scale according to configuration or and DB information.
        """
        database = self.pipeline.target

        if scale is None:
            if database.vendor == 'oracle':
                table = table.upper()
                schema = schema.upper()
                name = name.upper()

                code = [
                    f'SELECT data_scale FROM all_tab_columns@{link}',
                    f'WHERE owner = \'{schema}\'',
                    f'AND table_name = \'{table}\'',
                    f'AND column_name = \'{name}\'']
                code = '\n'.join(code)

                scale = database.connection.execute(code).scalar()

        return scale

    def compile_column_datatype(self, datatype, length, precision, scale):
        if datatype is sql.Numeric:
            datatype = datatype(precision, scale)
        elif datatype is sql.Integer:
            datatype = datatype
        elif datatype is sql.Float:
            datatype = datatype(precision)
        elif datatype is sql.String:
            datatype = datatype(length)
        elif datatype is sql.Date:
            datatype = datatype
        elif datatype is sql.DateTime:
            datatype = datatype
        elif datatype is sql.Time:
            datatype = datatype
        elif datatype is sql.Boolean:
            datatype = datatype
        return datatype

    def parse_primary_key(self):
        """
        Parse the primary key description from the configuration data and
        transform it to the sqlalchemy primary key definition expression.
        """
        primary_key = self.data.get('primary_key', [])
        if isinstance(primary_key, list) is True:
            keys = primary_key
            primary_key = sql.PrimaryKeyConstraint(*keys)
        elif isinstance(primary_key, dict) is True:
            name = primary_key.get('name')
            keys = primary_key.get('keys', [])
            primary_key = sql.PrimaryKeyConstraint(*keys, name=name)
        return primary_key

    def parse_foreign_keys(self):
        """
        Parse the foreign key description from the configuration data and
        transform it to the sqlalchemy foreign key definition expression.
        """
        fks_raw = self.data.get('foreign_keys')
        if isinstance(fks_raw, list) is True:
            fks = []
            for fk_raw in fks_raw:
                name = fk_raw.get('name')
                column = fk_raw.get('column')
                columns = fk_raw.get('columns')
                table = fk_raw.get('table')
                refcolumn = fk_raw.get('refcolumn', column)
                refcolumns = fk_raw.get('refcolumns', columns)

                if isinstance(column, str) is True:
                    if isinstance(refcolumn, str) is True:
                        refcolumn = f'{table}.{refcolumn}'
                        fk = sql.ForeignKeyConstraint(
                            [column], [refcolumn], name=name)
                elif isinstance(columns, list) is True:
                    if isinstance(refcolumns, list) is True:
                        refcolumns = [
                            f'{table}.{refcolumn}'
                            for refcolumn in refcolumns]
                        fk = sql.ForeignKeyConstraint(
                            columns, refcolumns, name=name)
                fks.append(fk)
            return fks
        else:
            return []

    def parse_params(self):
        """
        Parse all descriptions related to the table parameters from the
        configuration data and transform it to the sqlalchemy expressions.
        """
        vendor = self.pipeline.target.vendor
        data = self.data
        parameters = {}
        if vendor == 'oracle':
            if self.data.get('compress') is not None:
                compress = data['compress']
                parameters['oracle_compress'] = compress

        if self.data.get('parameters') is not None:
            other = data['parameters']
            if isinstance(other, dict) is True:
                parameters.update(other)
        return parameters

    def parse_query(self):
        """
        Parse the query described in the configuration data and transform it
        to the ANSI SQL expression.
        """
        query = self.data.get('query')
        if query is not None:
            query = []

            select = self.parse_select()
            from_ = self.parse_from()
            join = self.parse_join()
            where = self.parse_where()

            for part in [select, from_, join, where]:
                if part is not None:
                    query.append(part)

            query = '\n'.join(query)
            return query

    def parse_select(self):
        """
        Parse all descriptions related to the select part of the query
        from the configuration data and transform it to the SQL expression.
        """
        query = self.data.get('query')
        if query is not None:
            table = query.get('table')
            alias = query.get('alias')
            pointer = alias or table

            select_all = query.get('select_all', True)

            code = [r'SELECT']
            # Enable parallel execution for Oracle.
            if self.pipeline.target.vendor == 'oracle':
                hint = self.parse_oracle_parallel()
                if hint is not None:
                    code.append(hint)

            # Select all original fields or define the list of necessary fields.
            if select_all is True:
                code.append(f'{pointer}.*')
            elif select_all is False:
                fields = []
                columns = self.data.get('columns')
                for column in columns:
                    # Skip new fields because they are not in input.
                    load = column.get('load', True)
                    new = column.get('new', False)
                    if load is True and new is False:
                        column_name = column['name']
                        column_table = column.get('table')

                        column_pointer = column_table or pointer
                        field = [f'{column_pointer}.{column_name}']

                        trim = column.get('trim', False)
                        to_char = column.get('to_char', False)

                        if trim is True or to_char is True:
                            if trim is True:
                                field[0] = f'trim({field[0]})'
                            if to_char is True:
                                field[0] = f'to_char({field[0]})'
                            field.append('AS')
                            field.append(column_name)

                        field = ' '.join(field)
                        fields.append(field)
                fields = ', '.join(fields)
                code.append(fields)
            code = ' '.join(code)
            return code

    def parse_from(self):
        """
        Parse all descriptions related to the from part of the query
        from the configuration data and transform it to the SQL expression.
        """
        query = self.data.get('query')
        if query is not None:
            schema = query.get('schema')
            table = query.get('table')
            link = query.get('link')
            alias = query.get('alias')

            code = ['FROM']

            address = table
            if schema is not None:
                address = f'{schema}.{address}'
            if link is not None:
                address = f'{address}@{link}'
            code.append(address)

            if alias is not None:
                code.append(alias)

            code = ' '.join(code)
            return code

    def parse_join(self):
        """
        Parse all descriptions related to the join part of the query from the
        configuration data and transform it to the SQL expression.
        """
        query = self.data.get('query')
        if query is not None:
            join = query.get('join')
            if join is not None:
                schema = query.get('schema')
                table = query.get('table')
                link = query.get('link')
                alias = query.get('alias')

                pointer = alias or table

                code = []
                for data in join:
                    join_schema = data.get('schema', schema)
                    join_table = data.get('table')
                    join_link = data.get('link', link)
                    join_alias = data.get('alias')

                    type = data.get('type', 'inner')
                    using = data.get('using')
                    keys = data.get('keys')

                    modificator = type.upper()

                    address = join_table
                    if join_schema is not None:
                        address = f'{join_schema}.{address}'
                    if join_link is not None:
                        address = f'{address}@{join_link}'
                    if join_alias is not None:
                        address = f'{address} {join_alias}'

                    code.append(f'{modificator} JOIN {address}')
                    code.append('ON 1 = 1')

                    join_pointer = join_alias or join_table

                    if using is not None:
                        code.append(
                            f'AND {pointer}.{using} = {join_pointer}.{using}')
                    elif keys is not None:
                        for key in keys:
                            key_using = key.get('using')
                            key_table = key.get('table') or pointer
                            key_column = key.get('column')
                            operator = key.get('operator', '=').upper()

                            join_field = f'{join_pointer}.{key_using}'

                            if key_column is not None:
                                key_field = f'{key_table}.{key_column}'
                            elif key_using is not None:
                                key_field = f'{key_table}.{key_using}'

                            key_list = [
                                'AND', key_field, operator, join_field]
                            key_str = ' '.join(key_list)
                            code.append(key_str)

                code = '\n'.join(code)
                return code

    def parse_where(self):
        """
        Parse all description related to the where part of the query from
        the configuration data and transform it to the SQL expression inside
        the string.
        """
        period = self.parse_period()
        column_filters = self.parse_column_filters()
        query_filters = self.parse_query_filters()
        code = []
        for filter_ in [period, column_filters, query_filters]:
            if filter_ is not None:
                if len(code) == 0:
                    code.append('WHERE 1 = 1')
                code.append(filter_)
        code = '\n'.join(code)
        return code

    def parse_period(self):
        """
        Parse all description related to the date for which data is needed from
        the configuration data and transform it to the SQL expression inside
        the string.
        """
        pipeline = self.pipeline
        query = self.data.get('query')
        if isinstance(query, dict) is True:
            period = query.get('period')
            if isinstance(period, dict) is True:
                code = []
                filter_ = []

                main_table = query.get('table')
                main_alias = query.get('alias')

                table = period.get('table')
                column = period.get('column')
                using = period.get('using')
                value = period.get('value')
                utc = period.get('utc', False)
                starting = period.get('starting')
                searching = period.get('searching')

                pointer = table or main_alias or main_table
                column = column or using
                filter_.extend(["AND", f"{pointer}.{column}", "BETWEEN"])

                now = pipeline.run_timestamp
                format = 'YYYY-MM-DD HH24:MI:SS'
                if re.match(r'@Today', value) is not None:
                    begin = now.replace(hour=0, minute=0, second=0)
                    end = now.replace(hour=23, minute=59, second=59)
                if re.match(r'@Month', value) is not None:
                    begin = now.replace(day=1, hour=0, minute=0, second=0)
                    end = (begin + timedelta(days=32)).\
                        replace(day=1, hour=23, minute=59, second=59)\
                        - timedelta(days=1)
                elif re.match(r'@LastHour', value) is not None:
                    last_hour = now - timedelta(hours=1)
                    begin = last_hour.replace(minute=0, second=0)
                    end = last_hour.replace(minute=59, second=59)
                elif re.match(r'@LastDay', value) is not None:
                    last_day = now - timedelta(days=1)
                    begin = last_day.replace(hour=0, minute=0, second=0)
                    end = last_day.replace(hour=23, minute=59, second=59)
                elif re.match(r'@LastMonth', value) is not None:
                    last_month = (now.replace(day=1) - timedelta(days=1))
                    begin = last_month.\
                        replace(day=1, hour=0, minute=0, second=0)
                    end = last_month.\
                        replace(hour=23, minute=59, second=59)

                if starting is not None:
                    if pipeline.first_ever is True:
                        begin = datetime.fromisoformat(starting)

                if utc is True:
                    begin = begin.astimezone(tz=timezone.utc)
                    end = end.astimezone(tz=timezone.utc)

                filter_.extend([
                    f"TO_DATE('{begin:%Y-%m-%d %H:%M:%S}', '{format}')", "AND",
                    f"TO_DATE('{end:%Y-%m-%d %H:%M:%S}', '{format}')"])

                filter_ = ' '.join(filter_)
                if searching is None:
                    return filter_
                else:
                    code = []

                    subquery = []
                    s_select = ['SELECT DISTINCT']
                    s_fields = []
                    s_where = 'WHERE 1 = 1'
                    s_from_ = self.parse_from()
                    s_join = self.parse_join()

                    pointer = main_table or main_alias

                    for search in searching:
                        if isinstance(search, str) is True:
                            field = f'{pointer}.{search}'
                            s_fields.append(field)

                    s_fields = ', '.join(s_fields)
                    s_select.append(s_fields)
                    s_select = ' '.join(s_select)

                    for part in [s_select, s_from_, s_where, filter_, s_join]:
                        if part is not None:
                            subquery.append(part)
                    subquery = '\n'.join(subquery)

                    where = f'AND ({s_fields}) IN ('

                    code.extend([where, subquery, ')'])

                    code = '\n'.join(code)
                    return code

    def parse_column_filters(self):
        """
        Parse all descriptions related to column filters from
        the configuration data and transform it to the SQL expression inside
        the string.
        """
        query = self.data.get('query')
        if isinstance(query, dict) is True:
            table = query.get('table')
            alias = query.get('alias')
            pointer = alias or table
            columns = self.data.get('columns')
            if isinstance(columns, list)  is True:
                column_filters = []
                for column in columns:
                    name = column['name']
                    column_table = column.get('table', pointer)
                    filters = column.get('filters')
                    if isinstance(filters, list) is True:
                        for filter_ in filters:
                            filter_['column'] = name
                            filter_['table'] = column_table
                            filter_string = self._parse_filter(filter_)
                            column_filters.append(filter_string)
                column_filters = '\n'.join(column_filters)
                return column_filters

    def parse_query_filters(self):
        """
        Parse all descriptions related to query filters from
        the configuration data and transform it to the SQL expression inside
        the string.
        """
        query = self.data.get('query')
        if isinstance(query, dict) is True:
            table = query.get('table')
            alias = query.get('alias')
            pointer = alias or table
            filters = query.get('filters')
            query_filters = []
            if isinstance(filters, list) is True:
                for filter_ in filters:
                    if filter_.get('table') is None:
                        filter_['table'] = pointer
                    query_filter = self._parse_filter(filter_)
                    query_filters.append(query_filter)
                query_filters = '\n'.join(query_filters)
                return query_filters

    def _parse_filter(self, filter_):
        """
        Method to transform filter from configuration data to SQL expression
        inside the string.
        """
        if isinstance(filter_, dict) is True:
            filter_type = filter_.get('type', 'and')
            filter_table = filter_.get('table')
            filter_column = filter_.get('column')
            filter_operator = filter_.get('operator', '=')
            filter_value = filter_.get('value')

            filter_type = filter_type.upper()
            filter_operator = filter_operator.upper()

            if filter_value is None:
                filter_operator = 'IS'
                filter_value = 'NULL'
            elif isinstance(filter_value, list) is True:
                if filter_operator == 'BETWEEN'\
                and len(filter_value) == 2:
                    filter_value = (
                        f'{filter_value[0]} AND '\
                        f'{filter_value[1]}')
                else:
                    if filter_operator == '=':
                        filter_operator = 'IN'
                    filter_value = list(map(
                        lambda item: str(item)\
                            if isinstance(item, int) is True\
                            else f"'{item}'",
                        filter_value
                    ))
                    filter_value = ', '.join(filter_value)
                    filter_value = f'({filter_value})'
            elif isinstance(filter_value, str) is True:
                filter_value = f"'{filter_value}'"
            else:
                filter_value = str(filter_value)

            filter_list = [
                filter_type,
                f'{filter_table}.{filter_column}',
                filter_operator, filter_value]
            filter_string = ' '.join(filter_list)
            return filter_string

    def pick_medium_columns(self, only_names=False):
        table = self.pipeline.medium.data
        columns = self.data['columns']
        for column in columns:
            name = column['name']
            new = column.get('new', False)
            load = column.get('load', True)
            if new is False and load is True:
                if only_names is True:
                    yield name
                else:
                    yield table.c[name]

    def parse_table_name(self, type):
        pipeline = self.pipeline
        source = pipeline.source
        target = pipeline.target
        raw = pipeline.raw
        input = pipeline.input

        basename = self.data.get('rename') or raw.name or input.name
        pattern = '{type}_{source}_{basename}'
        cut = 3 + len(pipeline.source.name) + 2

        namings = self.data.get('namings')
        if isinstance(namings, dict) is True:
            custpattern = namings.get(type)
            if custpattern is not None:
                pattern = custpattern
                cut = 0

        if cut > 0:
            if target.vendor == 'oracle':
                maxlen = 30
            elif target.vendor == 'mysql':
                maxlen = 64
            elif target.vendor == 'postgresql':
                maxlen = 63
            else:
                maxlen = 255
            basename = basename[:maxlen-cut]

        name = pattern.format(
            type=type,
            basename=basename,
            source=source.name,
            target=target.name)
        return name

    def get_column(self, name, original=True):
        columns = self.data['columns']
        if isinstance(columns, list) is True:
            for column in columns:
                original_column = column['name']
                if original is True:
                    if name == original_column:
                        return column
                else:
                    target_column = column.get('rename', original_column)
                    if name == target_column:
                        return column

    def prepare(self):
        # Check fk configuration.
        if self.data.get('foreign_keys') is None:
            self.data['foreign_keys'] = []

        load_id = {'name': 'load_id', 'type': 'numeric', 'new': True}
        load_id_fk = {
            'column': 'load_id',
            'table': self.pipeline.log.table.name}
        self.data['columns'].append(load_id)
        self.data['foreign_keys'].append(load_id_fk)

        merge = self.data.get('merge')
        update = self.data.get('update')
        if isinstance(merge, dict) is True or isinstance(update, dict) is True:
            update_id = {'name': 'update_id', 'type': 'numeric', 'new': True}
            update_id_fk = {
                'column': 'update_id',
                'table': self.pipeline.log.table.name,
                'refcolumn': 'load_id'}
            self.data['columns'].append(update_id)
            self.data['foreign_keys'].append(update_id_fk)
        pass

    def parse_oracle_parallel(self):
        parallel = self.data.get('parallel', True)
        if parallel is True:
            dop = self.data.get('dop', 'auto')
            code = f'/*+ PARALLEL({dop}) */'
            return code
