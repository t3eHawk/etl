import json
import configparser
import sqlalchemy as sql
import sqlalchemy.orm as orm

from .dml import merge
from .func import trim
from .convs import naming_convention

class Database():
    def __init__(
        self, name, config=None, vendor=None, credentials=None,
        host=None, port=None, sid=None, user=None, password=None
    ):
        if config is not None and credentials is None:
            config = self.parse_config(config)
            section = name or 'DEFAULT'
            vendor = config[section].get('Vendor')
            host = config[section].get('Host')
            port = config[section].get('Port')
            sid = config[section].get('SID')
            user = config[section].get('User')
            password = config[section].get('Password')

        if credentials is None:
            credentials = f'{vendor}://{user}:{password}@{host}:{port}/{sid}'
        self.engine = sql.create_engine(credentials)
        self.session = orm.sessionmaker(bind=self.engine)()
        self.metadata = sql.MetaData(naming_convention=naming_convention)
        self.connection = self.engine.connect()


        self.name = name.lower()
        self.vendor = vendor.lower()
        self.schema = user.lower()

        self.compargs = {
            'bind': self.engine,
            'compile_kwargs': {
                'literal_binds': True}}
        pass

    def parse_config(self, path):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(path)
        return config

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

    # def inspect_dblink(self):
    #     inspect = 'SELECT db_link FROM user_db_links'
    #     pass
