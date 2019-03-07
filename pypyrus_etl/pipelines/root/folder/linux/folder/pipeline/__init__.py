import os

from datetime import datetime

from .tools import Log, Config, Parser
from .procs import Extractor, Transformer, Loader

class Pipeline():
    def __init__(
        self, name, source, object, target, config,
        run_timestamp=None, log=None, job=None
    ):
        self.name = name

        username = os.getlogin().upper()
        if job is not None:
            self.run_timestamp = job.trigger
            self.run_by = 'JOB' if job.auto is True else username
            self.job_id = job.id if job.auto is True else None
            if log is None:
                log = job.log
        else:
            self.run_timestamp = run_timestamp or datetime.now()
            self.run_by = username
            self.job_id = None

        self.source = source
        self.raw = object
        self.target = target

        self.log = Log(self, sys=log)
        self.config = Config(self, object=config)
        self.parser = Parser(self)

        self.extractor = Extractor(self)
        self.transformer = Transformer(self)
        self.loader = Loader(self)

        # Input folder.
        self.input = self.parser.parse_input()
        self.mediator = self.parser.parse_mediator()
        self.output = self.parser.parse_output()

        # Define is this load is first ever or not.
        self.first_ever = False
        self.with_update = False
        self.with_error = False
        pass

    def run(self):
        """Launch the ETL process form start to end."""
        self.prepare()
        self.extract()
        self.transform()
        self.load()
        self.finalize()
        pass

    def prepare(self):
        """
        Implement all necessary actions at the start of the ETL process.
        """
        self.log.sys.subhead('prepare')
        try:
            self.log.sys.info(f'Opening pipeline <{self.name}>...')
            self.input.prepare()
            self.mediator.prepare()
            self.output.prepare()
        except:
            self.log.sys.critical()
        else:
            self.log.sys.info('Preparation finished.')
        pass

    def extract(self):
        """Launch the EXTRACT part."""
        self.log.sys.subhead('extract')
        self.log.sys.info(f'Going to extract...')
        try:
            self.extractor.run()
        except:
            self.log.sys.critical()
        else:
            self.log.sys.info('Extraction finished.')
        pass

    def transform(self, *args):
        """Launch the TRANSFORM part."""
        self.log.sys.subhead('transform')
        self.log.sys.info(f'Going to transform...')
        try:
            self.transformer.run()
        except:
            self.log.sys.critical()
        else:
            self.log.sys.info('Transformation finished.')
        pass

    def load(self):
        """Launch the LOAD part."""
        self.log.sys.subhead('load')
        self.log.sys.info(f'Going to load...')
        try:
            self.loader.run()
        except:
            self.log.sys.critical()
        else:
            self.log.sys.info('Loading finished.')
        pass

    def finalize(self):
        """
        Implement all necessary actions at the end of the ETL process.
        """
        self.log.sys.bound()
        try:
            self.log.sys.info(f'Closing pipeline <{self.name}>...')
        except:
            self.log.sys.warning()
        else:
            self.log.sys.info('Done!')
        pass
