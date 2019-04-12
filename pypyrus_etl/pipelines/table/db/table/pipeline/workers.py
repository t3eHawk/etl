import re
import datetime as dt
import sqlalchemy as sql

class Operator():
    def __init__(self):
        self.records_found = None
        self.records_loaded = None
        self.records_updated = None
        self.records_error = None

        self._first_ever = False
        self._with_update = False
        self._with_error = False
        pass

    # def prepare_logtable(self):
    #     db = self.pipeline.output.database
    #     log = self.pipeline.log
    #     input = self.pipeline.input
    #     config = self.pipeline.config
    #
    #     tbname_by_default = f'log_{input.name}'
    #     if isinstance(config.get('settings'), dict) is True:
    #         tbname = config['settings'].get('logtable', tbname_by_default)
    #     else:
    #         tbname = tbname_by_default
    #
    #     table = sql.Table(
    #         tbname, db.metadata,
    #         sql.Column('load_id', sql.Integer, primary_key=True),
    #         sql.Column('job_timestamp', sql.Date),
    #         sql.Column('job_initiator', sql.String(40)),
    #         sql.Column('job_id', sql.Integer),
    #         sql.Column('start_timestamp', sql.Date),
    #         sql.Column('end_timestamp', sql.Date),
    #         sql.Column('records_found', sql.Integer),
    #         sql.Column('records_loaded', sql.Integer),
    #         sql.Column('records_updated', sql.Integer),
    #         sql.Column('records_error', sql.Integer),
    #         sql.Column('status', sql.Integer),
    #         oracle_compress=True)
    #     table.create(db.engine, checkfirst=True)
    #     self.logtable = table
    #     pass
    #
    # def prepare_logrecord(self):
    #     db = self.pipeline.output.database
    #     log = self.pipeline.log
    #     input = self.pipeline.input
    #     output = self.pipeline.output
    #     logtable = self.logtable
    #
    #     select = sql.select([sql.func.max(logtable.c.load_id)])
    #     result = db.connection.execute(select).scalar()
    #     self.load_id = 1 if result is None else result + 1
    #     self.status = 0
    #
    #     load_id = sql.literal(self.load_id).label('load_id')
    #     job_timestamp = self.pipeline.job_timestamp
    #     job_initiator = self.pipeline.job_initiator
    #     job_id = self.pipeline.job_identity
    #     start_timestamp = sql.func.sysdate()
    #     status = sql.literal(self.status).label('status')
    #
    #     insert = logtable.insert().values(
    #         load_id=load_id, job_timestamp=job_timestamp,
    #         job_initiator=job_initiator, job_id=job_id,
    #         start_timestamp=start_timestamp, status=status)
    #     db.connection.execute(insert)
    #
    #     log.info(
    #         f'Input is <{input.__class__.__name__}> '\
    #         f'with the name <{input.name}>')
    #     log.info(
    #         f'Output is <{output.__class__.__name__}> '\
    #         f'with the name <{output.name}>')
    #     log.info(
    #         f'That ETL pipeline owns id <{self.load_id}> '\
    #         f'in <{logtable.name}>.')
    #
    #     pass
    #
    # def prepare_medtable(self):
    #     db = self.pipeline.output.database
    #     log = self.pipeline.log
    #     input = self.pipeline.input
    #     output = self.pipeline.output
    #     config = self.pipeline.config
    #
    #     tbname_by_default = f'med_{input.name}'
    #     if isinstance(config.get('settings'), dict) is True:
    #         tbname = config['settings'].get('medtable', tbname_by_default)
    #     else:
    #         tbname = tbname_by_default
    #
    #     if db.engine.has_table(tbname) is True:
    #         log.debug(f'Table <{tbname}> already exists.')
    #         table = sql.Table(
    #             tbname, db.metadata,
    #             autoload=True, autoload_with=db.engine)
    #         drop = table.drop(db.engine)
    #         log.debug(drop)
    #         log.info(f'Table <{tbname}> dropped.')
    #
    #     create = [f'CREATE TABLE {tbname} AS']
    #     select = input.parse_select()
    #     from_ = input.parse_from()
    #     join = input.parse_join()
    #     for statement in [select, from_, join]:
    #         if statement is not None:
    #             create.append(statement)
    #     create.append('WHERE 1 = 0')
    #     create = '\n'.join(create)
    #     log.debug(create)
    #
    #     db.connection.execute(create)
    #     log.info(f'Table {tbname} created.')
    #
    #     table = sql.Table(
    #         tbname, db.metadata,
    #         autoload=True, autoload_with=db.engine)
    #
    #     self.medtable = table
    #     pass

    def _map_period(self):
        period = self.config['settings'].get('filter', {}).get('period')
        now = self.moment
        start = None
        end = None
        if period == 'Today':
            start = now.replace(hour=0, minute=0, second=0)
            end = now.replace(hour=23, minute=59, second=59)
        elif period == 'ThisMonth':
            start = now.replace(day=1, hour=0, minute=0, second=0)
            end = (start + dt.timedelta(days=32)).\
                replace(day=1, hour=23, minute=59, second=59)\
                - dt.timedelta(days=1)
        elif period == 'LastHour':
            last_hour = now - dt.timedelta(hours=1)
            start = last_hour.replace(minute=0, second=0)
            end = last_hour.replace(minute=59, second=59)
        elif period == 'Yesterday':
            last_day = now - dt.timedelta(days=1)
            start = last_day.replace(hour=0, minute=0, second=0)
            end = last_day.replace(hour=23, minute=59, second=59)
        elif period == 'LastMonth':
            last_month = (now.replace(day=1) - dt.timedelta(days=1))
            start = last_month.\
                replace(day=1, hour=0, minute=0, second=0)
            end = last_month.\
                replace(hour=23, minute=59, second=59)
        return (start, end)
