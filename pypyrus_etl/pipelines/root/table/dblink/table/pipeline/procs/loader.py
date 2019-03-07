import sqlalchemy as sql

import pypyrus_etl as etl

class Loader(etl.Loader):
    def run(self):
        """
        Insert to the target table only that columns that are requested
        in the configuration plus loading identification and only that
        rows that are not the duplicates of existing by primary key or keys.
        """
        output = self.pipeline.output
        medium = self.pipeline.medium

        config = self.pipeline.config

        # Get necessary items from the configuration.
        delete = config.data.get('delete', False)
        duplicates = config.data.get('duplicates', True)
        update = config.data.get('update')
        merge = config.data.get('merge')

        if delete is True:
            output.delete()
        elif delete is False:
            if duplicates is False:
                medium.process_duplicates()
            if len(output.data.primary_key) > 0:
                medium.process_primary_key_duplicates()

        if isinstance(merge, dict) is True:
            output.merge()
        elif isinstance(update, dict) is True:
            output.update()
        else:
            output.insert()
        pass
