import os
import json
import configparser
import sqlalchemy as sql
import sqlalchemy.orm as orm

from .dml import merge
from .func import trim
from .utils import parse_config
from .convs import naming_convention
from ...share import connections
from ...config import make_config

userconfig = os.path.abspath(os.path.expanduser('~/.pypyrus/db.json'))

def make_database(name):
    if connections.get(name) is not None:
        return connections.get(name)
    else:
        config = make_config(obj=userconfig)[name]
        vendor = config.get('vendor')
        host = config.get('host')
        port = config.get('port')
        sid = config.get('sid')
        user = config.get('user')
        password = config.get('password')
        database = Database(
            name=name, vendor=vendor, host=host, port=port, sid=sid,
            user=user, password=password)
        return database

class Database():
    def __init__(
        self, name=None, vendor=None, host=None, port=None, sid=None,
        user=None, password=None, credentials=None, config=None
    ):
        self.name = None
        self.vendor = None
        self.host = None
        self.port = None
        self.sid = None
        self.user = None
        self.schema = None
        self.engine = None
        self.session = None
        self.metadata = None
        self.connection = None
        self.compargs = None

        self.configure(
            name=name, vendor=vendor, host=host, port=port, sid=sid,
            user=user, password=password, credentials=credentials,
            config=config)
        pass

    def configure(
        self, name=None, vendor=None, host=None, port=None, sid=None,
        user=None, password=None, config=None, credentials=None
    ):
        if config is not None:
            if isinstance(name, str) is True:
                config = make_config(obj=config)[name]
                vendor = config.get('vendor', vendor)
                host = config.get('host', host)
                port = config.get('port', port)
                sid = config.get('sid', sid)
                user = config.get('user', user)
                password = config.get('password', password)

        if isinstance(name, str) is True:
            self.name = name.lower()
        if isinstance(vendor, str) is True:
            self.vendor = vendor.lower()
        if isinstance(host, str) is True:
            self.host = host
        if isinstance(port, (int, str)) is True:
            self.port = port
        if isinstance(sid, str) is True:
            self.sid = sid
        if isinstance(user, str) is True:
            self.user = self.schema = user.lower()

        if isinstance(credentials, str) is False:
            login = f'{self.user}:{password}'
            address = f'{self.host}:{self.port}/{self.sid}'
            credentials = f'{self.vendor}://{login}@{address}'

        if isinstance(password, str) is True and (
            host is not None or port is not None or sid is not None \
            or user is not None
        ):
            self.engine = sql.create_engine(credentials)
            self.session = orm.sessionmaker(bind=self.engine)()
            self.metadata = sql.MetaData(naming_convention=naming_convention)
            self.connection = self.engine.connect()
            # Share connections.
            connections[self.name] = self

        self.compargs = {
            'bind': self.engine, 'compile_kwargs': {'literal_binds': True}}
        pass

    def inspect(self, path=None, tech=False):
        """Get the structure of database objects."""
        inspector = sql.engine.reflection.Inspector.from_engine(self.engine)

        schemas = inspector.get_schema_names()
        data = {}
        schemas_ = {}
        for schema in schemas:
            schema_ = {}

            tables_ = {}
            tables = inspector.get_table_names(schema)
            for table in tables:
                table_ = {}

                if '$' in table and tech is False:
                    continue

                columns_ = []
                columns = inspector.get_columns(table, schema)
                for column in columns:
                    try:
                        name = column['name']
                        type = column['type']
                        column_ = f'{name} ({type})'
                        columns_.append(column_)
                    except:
                        continue

                table_['columns'] = columns_

                primary_key_ = {}
                primary_key = inspector.get_pk_constraint(table, schema)
                primary_key_name = primary_key['name']
                primary_key_columns = primary_key['constrained_columns']
                if primary_key_name is not None and primary_key_columns:
                    primary_key_[primary_key_name] = primary_key_columns
                table_['primary_key'] = primary_key_

                indexes_ = {}
                indexes = inspector.get_indexes(table, schema)
                for index in indexes:
                    name = index['name']
                    columns = index['column_names']
                    indexes_[name] = columns
                table_['indexes'] = indexes_

                foreign_keys_ = {}
                foreign_keys = inspector.get_foreign_keys(table, schema)
                for foreign_key in foreign_keys:
                    foreign_key_ = {}

                    name = foreign_key['name']
                    columns = foreign_key['constrained_columns']
                    referred_schema = foreign_key['referred_schema']
                    referred_table = foreign_key['referred_table']
                    referred_columns = foreign_key['referred_columns']

                    foreign_key_['columns'] = columns
                    foreign_key_['referred_schema'] = referred_schema
                    foreign_key_['referred_table'] = referred_table
                    foreign_key_['referred_columns'] = referred_columns

                    foreign_keys_[name] = foreign_key_

                table_['foreign_keys'] = foreign_keys_

                tables_[table] = table_

            schema_['tables'] = tables_

            views_ = []
            views = inspector.get_view_names(schema)
            for view in views:
                views_.append(view)
            schema_['views'] = views_

            schemas_[schema] = schema_

        data['schemas'] = schemas_

        if path is not None:
            with open(path, "w") as fp:
                json.dump(data, fp, indent=2, sort_keys=True)
        return data
