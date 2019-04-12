import os
import re

from datetime import datetime, timedelta

from ..objects import Input, Mediator, Output

class Parser():
    """
    That class represents the parser used to process some objects or
    parameters for different sources during the ETL process.
    """
    def __init__(self, pipeline):
        self.pipeline = pipeline
        pass

    def parse_input(self):
        """Parse configurator data to get input folder object."""
        pipeline = self.pipeline
        sftp = pipeline.source.sftp
        raw = pipeline.raw
        config = pipeline.config
        path = config['input']
        name = os.path.basename(path)
        object = Input(name, path=path, pipeline=pipeline)
        return object

    def parse_mediator(self):
        """Parse configurator data to get mediator folder object."""
        pipeline = self.pipeline
        sftp = pipeline.target.sftp
        config = pipeline.config
        path = config['mediator']
        name = 'none' if path is None else os.path.basename(path)
        object = Mediator(name, path=path, pipeline=pipeline)
        return object

    def parse_output(self):
        """Parse configurator data to get output folder object."""
        pipeline = self.pipeline
        sftp = pipeline.target.sftp
        config = pipeline.config
        path = config['output']
        name = os.path.basename(path)
        object = Output(name, path=path, pipeline=pipeline)
        return object

    def parse_files(self):
        pipeline = self.pipeline
        config = pipeline.config
        sftp = pipeline.source.sftp
        path = pipeline.input.path
        namemask = config['filename']
        period = config['period']
        files = []
        for filename in sftp.listdir(path):
            filepath = f'{path}/{filename}'
            if period is not None:
                now = pipeline.run_timestamp
                stat = sftp.stat(filepath)
                mtime = datetime.fromtimestamp(stat.st_mtime)
                if period == 'today':
                    start = now.replace(hour=0, minute=0, second=0)
                    end = now.replace(hour=23, minute=59, second=59)
                elif period == 'yesterday':
                    then = now - timedelta(days=1)
                    start = then.replace(hour=0, minute=0, second=0)
                    end = then.replace(hour=23, minute=59, second=59)
                elif period == 'this-hour':
                    start = now.replace(minute=0, second=0)
                    end = now.replace(minute=59, second=59)
                elif period == 'last-hour':
                    then = now - timedelta(hours=1)
                    start = then.replace(minute=0, second=0)
                    end = then.replace(minute=59, second=59)
                elif period == 'this-month':
                    start = now.replace(day=1, hour=0, minute=0, second=0)
                    end = (start + timedelta(days=32)).\
                        replace(day=1, hour=23, minute=59, second=59)\
                        - timedelta(days=1)
                elif period == 'last-month':
                    then = (now.replace(day=1) - timedelta(days=1))
                    start = then.replace(day=1, hour=0, minute=0, second=0)
                    end = then.replace(hour=23, minute=59, second=59)
                if not (mtime >= start and mtime <= end):
                    break
            if namemask is not None:
                if re.match(namemask, filename) is None:
                    break
            file = {
                'name': filename,
                'path': filepath,
                'size': sftp.stat(filepath).st_size}
            files.append(file)
        return files
