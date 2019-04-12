from .workers import Operator
from ......utils import str_to_dttm, dttm_to_utc
from ......pipeline import Setup

from sqlalchemy.ext.automap import automap_base

class Pipeline(Setup, Operator):
    def __init__(self, input, output, *args, **kwargs):
        # Initialize core attributes.
        super().__init__(input, output, **kwargs)

        # Process all input table configuration.
        if isinstance(self.config.get('input'), dict) is True:
            if input.__class__.__name__ == 'Table':
                attrs = [
                    'name', 'schema', 'dblink', 'alias', 'columns', 'joins',
                    'select_all', 'parallel']
                for attr in attrs:
                    if self.config['input'].get(attr) is not None:
                        self.log.debug(f'input.{attr} taken from config.')
                        value = self.config['input'][attr]
                        setattr(input, attr, value)

        # Add filter to input table if required.
        if isinstance(self.config.get('settings'), dict) is True:
            filter = self.config['settings'].get('filter')
            if isinstance(filter, dict) is True:
                attribute = filter.get('attribute')
                self.log.info(f'Apply input data filter using <{attribute}>')
                utc = filter.get('utc', False)
                starting = filter.get('starting')

                # Map period from config to datetime values.
                start, end = self._map_period()
                # Adjust period if needed.
                if start is not None and end is not None:
                    if starting is not None and self.first_ever is True:
                        start = str_to_dttm(starting)
                    if utc is True:
                        start = dttm_to_utc(start)
                        end = dttm_to_utc(end)

                    # Finally convert periods to ISO strings.
                    start = start.isoformat(sep=' ', timespec='seconds')
                    end = end.isoformat(sep=' ', timespec='seconds')
                    self.log.info(f'With <start> as {start}')
                    self.log.info(f'And with <end> as {end}')
                    # And then convert to SQL dates.
                    format = 'YYYY-MM-DD HH24:MI:SS'
                    start = f"TO_DATE('{start}', '{format}')"
                    end = f"TO_DATE('{end}', '{format}')"
                    value = [start, end]
                    column = {
                        'name': attribute,
                        'filters': [{'operator': 'between', 'value': value}],
                        'select': False
                    }
                    self.input.columns.append(column)

        # Process all output table configuration.
        if isinstance(self.config.get('output'), dict) is True:
            if output.__class__.__name__ == 'Table':
                attrs = [
                    'name', 'schema', 'dblink', 'columns', 'compress',
                    'primary_key', 'foreign_key']
                for attr in attrs:
                    if self.config['output'].get(attr) is not None:
                        self.log.debug(f'output.{attr} taken from config.')
                        value = self.config['output'][attr]
                        setattr(output, attr, value)

        # Check input table select columns.
        if self.input.select_all is False:
            for column in self.output.columns:
                name = column['name']
                source = column.get('source', name)
                literal = column.get('literal', False)

        pass

    def prepare(self):
        pass

    def extract(self):
        import queue
        import threading

        self.db = self.input.database

        self.output.create()
        self.insert = self.output.entity.insert()

        self.queue = queue.Queue()
        threads = 1
        for i in range(threads):
            thread = threading.Thread(target=self.process_queue, daemon=True)
            thread.start()

        query = self.input.parse_query()
        result = self.db.connection.execute(query)

        commit = 50000
        self.log.info('Fetching records...')
        while True:
            chunk = result.fetchmany(commit)
            count = len(chunk)
            self.input_count += count
            if chunk:
                self.queue.put(chunk)
                i += 1
            else:
                break

        self.queue.join()
        pass

    def process_queue(self):
        while True:
            chunk = self.queue.get()
            self.db.connection.execute(self.insert, chunk)
            count = len(chunk)
            self.output_count += count
            self.log.info(f'{self.output_count} records inserted.')
            self.queue.task_done()
        pass

    # def extract(self):
    #     db = self.input.database
    #
    #     self.output.create()
    #     insert = self.output.entity.insert()
    #
    #     query = self.input.parse_query()
    #     result = db.connection.execute(query)
    #
    #     commit = 100000
    #     while True:
    #         chunk = result.fetchmany(commit)
    #         self.log.info('Fetched records.')
    #         if chunk:
    #             db.connection.execute(insert, chunk)
    #             count = len(chunk)
    #             self.output_count += count
    #             self.log.info(f'{self.output_count} records inserted.')
    #         else:
    #             break
    #     pass

    def load(self):
        pass

    def finalize(self):
        pass
