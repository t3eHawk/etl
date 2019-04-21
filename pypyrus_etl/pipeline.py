from .log import make_log, log_columns
from .utils import who, when
from .config import make_config
from .items.table import Table
from .nodes.database import Database

class Pipeline():
    def __new__(self, input, output, *args, **kwargs):
        if isinstance(input, Table) is True:
            if isinstance(output, Table) is True:
                from .pipelines.table.db.table.pipeline import Pipeline
        return Pipeline(input, output, *args, **kwargs)

class Setup():
    def __init__(
        self, input, output, name=None, config=None, log=None, job=None,
        moment=None, **kwargs
    ):
        self.input = input
        self.output = output

        # Name of the pipeline. If name was not defined and pipeline is a part
        # of a job then take its name.
        if name is None:
            if job is not None and hasattr(job, 'name') is True:
                name = job.name

        # System logger. If logger was not passed and pipeline is a part of
        # a job then try to take its logger. If still no logger was defined
        # then use main logger.
        if log is None:
            if job is not None and hasattr(job, 'log') is True:
                log = job.log

        if moment is None:
            if job is not None and hasattr(job, 'moment') is True:
                moment = job.moment

        # The username who initiated the pipeline. If part of the job then
        # take its initiator that can be either username or scheduler.
        initiator = None
        if job is not None and hasattr(job, 'initiator') is True:
            initiator = job.initiator

        # The name of the pipeline.
        self.name = name
        # Identity of job that was used to deploy this pipeline.
        self.job = job
        # Logger used for pipeline.
        self.log = make_log(obj=log)

        # Process configurator.
        self.config = make_config(obj=config)

        self.oper_id = None
        # Moment for which pipeline initiated.
        self.moment = moment or when()
        # Time that reflects data period.
        self.oper_timestamp = self.moment
        # Who initiated the pipeline.
        self.initiator = initiator or who()
        # Time of pipeline execution
        self.start_timestamp = None
        self.end_timestamp = None
        # Count statistics of input and output objects.
        self.input_count = 0
        self.output_count = 0
        self.update_count = 0
        self.error_count = 0
        # Pipeline process.
        self.status = None

        # Process table logging configuration.
        if self.log.table.status is False:
            if isinstance(self.config.get('logging'), dict) is True:
                config = self.config.get('logging')
                if config.get('columns') is None:
                    config['columns'] = log_columns
                table = Table(config=config)
                if table.exists is False:
                    table.create()
                else:
                    table.load()
                self.log.table.configure(
                    db=table.db.connection, entity=table.entity)
                self.log.table.open()
                self.log.set(
                    oper_timestamp=self.oper_timestamp,
                    initiator=self.initiator,
                    job=job.id if hasattr(self.job, 'id') is True else None)
                self.oper_id = self.log.table.pointer
                self.log.sysinfo.prms['oper_id'] = self.oper_id
        pass

    def run(self):
        self._prepare()
        self._execute()
        self._finalize()
        pass

    def prepare(self):
        pass

    def execute(self):
        pass

    def finalize(self):
        pass

    def _prepare(self):
        self.log.bound()
        self.log.info(f'Opening pipeline <{self.name}>...')
        self.log.info(f'Operation ID <{self.oper_id}>')
        self.log.info(f'Operation timestamp <{self.oper_timestamp}>')
        try:
            self.prepare()
            self.start_timestamp = when()
            self.log.info(f'Started at {self.start_timestamp}')
            self.log.set(start_timestamp=self.start_timestamp)
        except:
            self.log.critical()
        else:
            self.log.info('Pipeline opened')
        pass

    def _execute(self):
        self.log.subhead('execution')
        try:
            self.status = 'P'
            self.log.set(status=self.status)
            self.execute()
        except:
            self.status = 'E'
            self.log.set(status=self.status)
            self.log.critical()
        else:
            self.status = 'D'
            self.log.set(status=self.status)
            self.log.info('Execution finished')
        pass

    def _finalize(self):
        self.log.bound()
        self.log.info(f'Closing pipeline <{self.name}>...')
        try:
            self.finalize()
            self.end_timestamp = when()
            self.log.info(f'Ended at {self.end_timestamp}')
            self.log.set(end_timestamp=when())
        except:
            self.log.critical()
        else:
            self.log.info('Pipeline closed')
        pass
