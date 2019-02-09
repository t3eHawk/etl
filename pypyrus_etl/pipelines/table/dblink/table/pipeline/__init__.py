import os
import sqlalchemy as sql

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

        # Source table on original source.
        self.input = self.parser.parse_input()
        # Medium table for raw data initially loaded from original source.
        self.medium = self.parser.parse_medium()
        # Target data source table in which records will be loaded.
        self.output = self.parser.parse_output()

        # Define is this load is first ever or not.
        self.first_ever = not target.engine.has_table(self.output.name)
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
            # Open new record in the DB log.
            self.log.open()
            # Complete configurator.
            self.config.prepare()
            # Prepare medium table object.
            self.medium.prepare()
            # Prepare output table object.
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
            self.log.process_error()
        else:
            self.log.process_extract_finished()
        pass

    def transform(self, *args):
        """Launch the TRANSFORM part."""
        self.log.sys.subhead('transform')
        self.log.sys.info(f'Going to transform...')
        try:
            self.transformer.run()
        except:
            self.log.process_error()
        else:
            self.log.process_transform_finished()
        pass

    def load(self):
        """Launch the LOAD part."""
        self.log.sys.subhead('load')
        self.log.sys.info(f'Going to load...')
        try:
            self.loader.run()
        except:
            self.log.process_error()
        else:
            self.log.process_load_finished()
        pass

    def finalize(self):
        """
        Implement all necessary actions at the end of the ETL process.
        """
        self.log.sys.bound()
        try:
            self.log.sys.info(f'Closing pipeline <{self.name}>...')
            self.log.close()
        except:
            self.log.sys.warning()
        self.log.sys.info('Done!')
        pass

# class Pipelines():
#     """
#     This class represents set of pipelines where each separated pipeline has
#     own configuration json file stored in special folder.
#     """
#     def __init__(self, source, target, folder, log=None):
#         self.pipelines = []
#         for tbname in os.listdir(folder):
#
#             json = f'{folder}/{tbname}/{tbname}.json'
#             json = json if os.path.exists(json) is True else None
#
#             ini = f'{folder}/{tbname}/{tbname}.ini'
#             ini = ini if os.path.exists(ini) is True else None
#
#             object = etl.table.Table(tbname, json=json, ini=ini)
#             pipeline = Pipeline(source, target, object, log=log)
#             self.pipelines.append(pipeline)
#         pass
#
#     def __getitem__(self, key):
#         return self.pipelines[key]
#
#     def __iter__(self):
#         self.max = len(self.pipelines)
#         self.n = 0
#         return self
#
#     def __next__(self):
#         n = self.n
#         if self.n < self.max:
#             self.n += 1
#             return self.pipelines[n]
#         else:
#             raise StopIteration
#
#     def run_all(self):
#         """Launch all nested ETL pipelines."""
#         for pipeline in self.pipelines:
#             pipeline.run()
#         pass
