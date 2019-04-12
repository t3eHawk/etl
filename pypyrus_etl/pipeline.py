from .log import Logger
from .utils import who, when
from .config import Configurator
from .items.table import Table

class Pipeline():
    def __new__(self, input, output, *args, **kwargs):
        if isinstance(input, Table) is True:
            if isinstance(output, Table) is True:
                if id(input.database) == id(output.database):
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
        self.log = log or Logger()

        # Process configurator.
        try:
            if config is None:
                self.log.warning('Configurator was not found!')
                config = {}
            elif isinstance(config, str) is True:
                self.log.debug('Configurator is JSON file.')
                config = Configurator(path=config)
            elif isinstance(config, dict) is True:
                self.log.debug('Configurator is dictionary.')
        except:
            self.log.critical()
        else:
            self.config = config
            if len(config.keys()) > 0:
                self.log.debug('Configurator parsed.')
            else:
                self.log.warning('Configurator is empty!')

        # Who initiated the pipeline.
        self.initiator = initiator or who()
        # moment for which pipeline initiated.
        self.moment = moment or when()
        # Time of pipeline execution
        self.start_timestamp = None
        self.end_timestamp = None
        # Count statistics of input and output objects.
        self.input_count = 0
        self.output_count = 0
        # Pipeline process.
        self.status = None
        pass

    def run(self):
        self._prepare()
        self._extract()
        self._transform()
        self._load()
        self._finalize()
        pass

    def prepare(self):
        pass

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    def finalize(self):
        pass

    def _prepare(self):
        """
        Implement all necessary actions at the start of the ETL process.
        """
        self.log.bound()
        self.log.info(f'Opening pipeline <{self.name}>...')
        try:
            self.prepare()
        except:
            self.log.error()
        else:
            self.log.info('DONE')
        pass

    def _extract(self):
        self.log.subhead('extraction')
        try:
            self.extract()
        except:
            self.log.error()
        else:
            self.log.info('DONE')
        pass

    def _transform(self, *args):
        self.log.subhead('transformation')
        try:
            self.transform()
        except:
            self.log.error()
        else:
            self.log.info('DONE')
        pass

    def _load(self):
        self.log.subhead('loading')
        try:
            self.load()
        except:
            self.log.error()
        else:
            self.log.info('DONE')
        pass

    def _finalize(self):
        self.log.bound()
        self.log.info(f'Closing pipeline <{self.name}>...')
        try:
            self.finalize()
        except:
            self.log.warning()
        else:
            self.log.info('Done!')
        pass
