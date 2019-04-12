from sqlalchemy.sql.schema import Table
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement, FunctionElement

# CLASSES FOR COMPILATIONS.

class merge(Executable, ClauseElement):
    def __init__(self, table, using, keys, updcols, inscols, prefixes=None):
        self.table = table

        if isinstance(using, Table) is True:
            using = using.select()
        self.using = using

        self._keys = keys
        keys = []
        for key in self._keys:
            if isinstance(key, str) is True:
                key = key.lower()
                keys.append(f't.{key} = u.{key}')
            elif isinstance(key, list) is True:
                if len(key) == 2:
                    table_key = key[0].lower()
                    using_key = key[1].lower()
                    keys.append(f't.{table_key} = u.{using_key}')
        self.keys = keys

        self.updcols = updcols
        updates = []
        for column in updcols:
            if isinstance(column, str) is True:
                column = column.lower()
                updates.append(f't.{column} = u.{column}')
            elif isinstance(column, list) is True:
                if len(column) == 2:
                    table_column = column[0].lower()
                    using_column = column[1].lower()
                    updates.append(
                        f't.{table_column} = u.{using_column}')
        self.updates = updates

        self.inscols = inscols
        values = []
        inserts = []
        for column in inscols:
            if isinstance(column, str) is True:
                column = column.lower()
                values.append(f't.{column}')
                inserts.append(f'u.{column}')
            elif isinstance(column, list) is True:
                if len(column) == 2:
                    values_column = column[0].lower()
                    insert_column = column[1].lower()
                    values.append(f't.{values_column}')
                    inserts.append(f'u.{insert_column}')
        self.values = values
        self.inserts = inserts

        if prefixes is None:
            self.prefixes = [' ']
        elif isinstance(prefixes, list) is True:
            self.prefixes = prefixes
        elif isinstance(prefixes, str) is True:
            self.prefixes = [f' {prefixes} ']

        stmt = [
            'MERGE{prefixes}INTO {table} t',
            'USING ({using}) u',
            'ON ({keys})',
            'WHEN MATCHED THEN UPDATE SET {updates}',
            'WHEN NOT MATCHED THEN INSERT ({values}) VALUES ({inserts})']
        self.stmt = '\n'.join(stmt)
        pass

    def prefix_with(self, prefixes, dialect=None):
        table = self.table
        using = self.using
        keys = self._keys
        updcols = self.updcols
        inscols = self.inscols

        allow = True
        if self.bind is not None and dialect is not None:
            if self.bind.dialect.name != dialect:
                allow = False

        if allow is True:
            return merge(
                table, using, keys, updcols, inscols, prefixes=prefixes)
        else:
            return self

class update(Executable, ClauseElement):
    def __init__(self, table, using, keys, columns, prefixes=None):
        self.table = table

        if isinstance(using, Table) is True:
            using = using.select()
        self.using = using

        self._keys = keys
        keys = []
        for key in self._keys:
            if isinstance(key, str) is True:
                key = key.lower()
                keys.append(f't.{key} = u.{key}')
            elif isinstance(key, list) is True:
                if len(key) == 2:
                    table_key = key[0].lower()
                    using_key = key[1].lower()
                    keys.append(f't.{table_key} = u.{using_key}')
        self.keys = keys

        self._columns = columns
        columns = []
        for column in self._columns:
            if isinstance(column, str) is True:
                column = column.lower()
                columns.append(f't.{column} = u.{column}')
            elif isinstance(column, list) is True:
                if len(column) == 2:
                    table_column = column[0].lower()
                    using_column = column[1].lower()
                    columns.append(
                        f't.{table_column} = u.{using_column}')
        self.columns = columns

        if prefixes is None:
            self.prefixes = [' ']
        elif isinstance(prefixes, list) is True:
            self.prefixes = prefixes
        elif isinstance(prefixes, str) is True:
            self.prefixes = [f' {prefixes} ']

        merge_stmt = [
            'MERGE{prefixes}INTO {table} t',
            'USING ({using}) u',
            'ON ({keys})',
            'WHEN MATCHED THEN UPDATE SET {columns}']
        self.merge_stmt = '\n'.join(merge_stmt)
        pass

    def prefix_with(self, prefixes, dialect=None):
        table = self.table
        using = self.using
        keys = self._keys
        columns = self._columns

        allow = True
        if self.bind is not None and dialect is not None:
            if self.bind.dialect.name != dialect:
                allow = False

        if allow is True:
            return update(table, using, keys, columns, prefixes=prefixes)
        else:
            return self

class trim(FunctionElement):
    name = 'trim'

def parallel(stmt, dop='auto'):
    hint = f'/*+ PARALLEL({dop}) */'
    return stmt.prefix_with(hint, dialect='oracle')

# COMPILATIONS

@compiles(merge)
def compile(element, compiler, **kwargs):
    pass

@compiles(merge, 'oracle', 'postgresql')
def compile(element, compiler, **kwargs):
    kwargs['literal_binds'] = True
    return element.stmt.format(
        table=compiler.process(element.table, asfrom=True, **kwargs),
        using=compiler.process(element.using, **kwargs),
        keys='\nAND '.join(element.keys),
        updates=', '.join(element.updates),
        values=', '.join(element.values),
        inserts=', '.join(element.inserts),
        prefixes=' '.join(element.prefixes))

@compiles(update)
def compile(element, compiler, **kwargs):
    pass

@compiles(update, 'oracle', 'postgresql')
def compile(element, compiler, **kwargs):
    kwargs['literal_binds'] = True
    return element.merge_stmt.format(
        table=compiler.process(element.table, asfrom=True, **kwargs),
        using=compiler.process(element.using, **kwargs),
        keys='\nAND '.join(element.keys),
        columns=', '.join(element.columns),
        prefixes=' '.join(element.prefixes))

@compiles(trim)
def compile(element, compiler, **kwargs):
    return "trim(%s)" % compiler.process(element.clauses, **kwargs)
