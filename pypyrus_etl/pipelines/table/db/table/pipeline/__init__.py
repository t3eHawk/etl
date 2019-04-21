from .workers import Operator
from ......queue import queue
from ......utils import str_to_dttm, dttm_to_utc
from ......pipeline import Setup

class Pipeline(Setup, Operator):
    def __init__(self, input, output, *args, **kwargs):
        # Initialize core attributes.
        super().__init__(input, output, **kwargs)

        # Process all input table configuration.
        if isinstance(self.config.get('input'), dict) is True:
            self.input.configure(config=self.config.get('input'))

        # Process all output table configuration.
        if isinstance(self.config.get('output'), dict) is True:
            self.output.configure(config=self.config.get('output'))

        pass

    def prepare(self):
        if self.output.exists is False:
            self.output.create()
            self._first_ever = True
        else:
            self.output.load()

        # Map output columns on input columns.
        self.log.debug('Map columns...')
        for column in self.output.columns:
            name = column['name']
            source = column.get('source')
            if source is None:
                column['source'] = name
                self.log.debug(f'{name} --> {name}')
            elif source is not False:
                self.log.debug(f'{source} --> {name}')

        # Fill in input columns if necessary.
        if self.input.select_all is False and len(self.input.columns) == 0:
            self.log.debug('Input columns are not defined')
            for column in self.output.columns:
                source = column['source']
                if isinstance(source, str) is True:
                    self.input.columns.append({'name': source, 'select': True})
                    self.log.debug(f'Column {source} added to input')
                elif isinstance(source, dict) is True:
                    input_column = {}
                    input_column['name'] = source['name']
                    if source.get('table') is not None:
                        input_column['table'] = source['table']
                    if source.get('value') is not None:
                        input_column['value'] = source['value']
                    if source.get('trim') is not None:
                        input_column['trim'] = source['trim']
                    if source.get('to_char') is not None:
                        input_column['to_char'] = source['to_char']
                    self.input.columns.append(input_column)
                    self.log.debug(f"Column {source['name']} added to input")

        # Add filter to input table if required.
        if isinstance(self.config.get('meta'), dict) is True:
            filter = self.config['meta'].get('filter')
            if isinstance(filter, dict) is True:
                attribute = filter.get('attribute')
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
                    start = start.strftime('%Y-%m-%d %H:%M:%S')
                    end = end.strftime('%Y-%m-%d %H:%M:%S')
                    self.log.info(
                        f'Defined <{attribute}> filter: {start} --> {end}')
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
                    self.log.debug('Filter applied')
        pass

    def execute(self):
        no_fetch = self.config['meta'].get('no_fetch', False)
        merge = self.config['meta'].get('merge', False)
        if no_fetch is False:
            self.output.load()
            self.input.extract()
            self.queue.join()
        elif merge if False:
            self.input.select_count()
            self.log.set(input_count=self.input.count)
            self.output.insert_select(self.input.query)
            self.log.set(output_count=self.input.count)
        pass
