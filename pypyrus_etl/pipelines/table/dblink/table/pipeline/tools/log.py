import pypyrus_logbook as logbook
import sqlalchemy as sql

class Log():
    """
    That class represents the log object of an ETL process for objects loaded
    from the source to the target database using dblink.
    """
    def __init__(self, pipeline, sys=None):
        self.pipeline = pipeline
        self.sys = sys or logbook.Log('pipeline')
        pass

    def prepare(self):
        """Get DB table logger."""
        db = self.pipeline.target
        config = self.pipeline.config

        tbname = config.parse_table_name('log')

        table = sql.Table(
            tbname, self.pipeline.target.metadata,
            sql.Column('load_id', sql.Integer, primary_key=True),
            sql.Column('run_timestamp', sql.Date),
            sql.Column('run_by', sql.String(40)),
            sql.Column('job_id', sql.Integer),
            sql.Column('start_timestamp', sql.Date),
            sql.Column('end_timestamp', sql.Date),
            sql.Column('records_found', sql.Integer),
            sql.Column('records_loaded', sql.Integer),
            sql.Column('records_updated', sql.Integer),
            sql.Column('records_error', sql.Integer),
            sql.Column('status', sql.String(1)),
            oracle_compress=True)
        table.create(self.pipeline.target.engine, checkfirst=True)
        self.table = table
        pass

    def open(self):
        """
        Open logging for current ETL process by adding new record in the LOG.
        """
        pipeline = self.pipeline
        source = pipeline.source
        raw = pipeline.raw
        target = pipeline.target

        self.prepare()
        # Id for current ETL process.
        self.load_id = self.calculate_id()
        # Current status of the ETL process.
        self.status = 0

        load_id = sql.literal(self.load_id).label('load_id')
        run_timestamp = pipeline.run_timestamp
        run_by = pipeline.run_by
        job_id = pipeline.job_id
        start_timestamp = sql.func.sysdate()
        status = sql.literal(self.status).label('status')

        insert = self.table.insert().values(
            load_id=load_id, run_timestamp=run_timestamp, run_by=run_by,
            job_id=job_id, start_timestamp=start_timestamp, status=status)
        target.connection.execute(insert)

        self.sys.info(
            f'Source is <{source.__class__.__name__}> '\
            f'with the name <{source.name}>')
        self.sys.info(
            f'Target is <{target.__class__.__name__}> '\
            f'with the name <{target.name}>')
        self.sys.info(
            f'Object is <{raw.__class__.__name__}> '\
            f'with the name <{raw.name}>')
        self.sys.info(
            f'That ETL pipeline owns id <{self.load_id}> '\
            f'in <{target.name}.{target.schema}.{self.table.name}>.')

        pass

    def process_extract_finished(self):
        """Set status to 1 and count records found in the input table."""
        db = self.pipeline.target

        self.status = 1
        self.records_found = self.calculate_records_found()

        load_id = self.load_id
        status = sql.literal(self.status).label('status')
        records_found = self.records_found

        self.sys.info(f'Extracted records <{records_found}>.')
        update = self.table.update().\
            where(self.table.c.load_id == load_id).\
            values(status=status, records_found=records_found)
        db.connection.execute(update)

        self.sys.info('Extraction finished.')
        pass

    def process_transform_finished(self):
        """Set status to 2."""
        db = self.pipeline.target

        self.status = 2

        load_id = self.load_id
        status = sql.literal(self.status).label('status')

        update = self.table.update().\
            where(self.table.c.load_id == load_id).\
            values(status=status)
        db.connection.execute(update)

        self.sys.info('Transformation finished.')
        pass

    def process_load_finished(self):
        """Set status to 3 and count records loaded to output table."""
        db = self.pipeline.target

        self.status = 3
        self.records_loaded = self.calculate_records_loaded()
        self.records_updated = self.calculate_records_updated()
        self.records_error = self.calculate_records_error()

        load_id = self.load_id
        status = sql.literal(self.status).label('status')
        records_loaded = self.records_loaded
        records_updated = self.records_updated
        records_error = self.records_error

        self.sys.info(f'Loaded records <{records_loaded}>.')
        self.sys.info(f'Updated records <{records_updated or 0}>.')
        self.sys.info(f'Error records <{records_error or 0}>.')
        update = self.table.update().\
            where(self.table.c.load_id == load_id).\
            values(
                status=status, records_loaded=records_loaded,
                records_updated=records_updated, records_error=records_error)
        db.connection.execute(update)

        self.sys.info('Loading finished.')
        pass

    def process_error(self):
        """Set status to 4 and break the execution."""
        db = self.pipeline.target

        self.status = 4

        load_id = self.load_id
        status = sql.literal(self.status).label('status')

        update = self.table.update().\
            where(self.table.c.load_id == load_id).\
            values(status=status)
        self.sys.critical()
        pass

    def close(self):
        """
        Close logging for current ETL process by updating end_timestamp and
        status in the LOG.
        """
        load_id = sql.literal(self.load_id).label('load_id')
        end_timestamp = sql.func.sysdate()
        status = sql.literal(self.status).label('status')

        update = self.table.update().\
            where(self.table.c.load_id == load_id).\
            values(end_timestamp=end_timestamp, status=status)

        self.pipeline.target.connection.execute(update)
        pass

    def update_status(self, status=None):
        """Simple way to modify status for current ETL process."""
        if status is not None:
            self.status = status

            load_id = sql.literal(self.load_id).label('load_id')
            status = sql.literal(self.status).label('status')

            update = self.table.update().\
                where(self.table.c.load_id == load_id).\
                values(status=status)

            self.pipeline.target.connection.execute(update)
        pass

    def calculate_id(self):
        """Calculate load id for current ETL process."""
        db = self.pipeline.target

        # Select last known id form the log table.
        select = sql.select(
            [sql.func.max(self.table.c.load_id).label('load_id')])
        result = db.connection.execute(select).scalar()
        # If no id was found in the log table then that is the first ETL
        # process for and that load must have id 0.
        load_id = 0 if result is None else result + 1
        return load_id

    def calculate_records_found(self):
        """Count the records found in input table."""
        pipeline = self.pipeline
        db = pipeline.target
        table = pipeline.medium.data

        count = sql.select([sql.func.count()]).select_from(table)
        result = db.connection.execute(count).scalar()
        return result

    def calculate_records_loaded(self):
        """Count the records loaded to output table on this load."""
        pipeline = self.pipeline
        db = pipeline.target
        table = pipeline.output.data

        load_id = sql.literal(self.load_id).label('load_id')
        count = sql.select([sql.func.count()]).select_from(table).\
            where(table.c.load_id == load_id)
        result = db.connection.execute(count).scalar()
        return result

    def calculate_records_updated(self):
        pipeline = self.pipeline
        with_update = pipeline.with_update
        if with_update is True:
            db = pipeline.target
            table = pipeline.output.data
            load_id = sql.literal(self.load_id).label('load_id')
            count = sql.select([sql.func.count()]).select_from(table).\
                where(table.c.update_id == load_id)
            result = db.connection.execute(count).scalar()
            return result

    def calculate_records_error(self):
        pipeline = self.pipeline
        with_error = pipeline.with_error
        if with_error is True:
            db = pipeline.target
            table = pipeline.error_handler.data
            load_id = sql.literal(self.load_id).label('load_id')
            count = sql.select([sql.func.count()]).select_from(table).\
                where(table.c.load_id == load_id)
            result = db.connection.execute(count).scalar()
            return result
