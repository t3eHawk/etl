import pypyrus_etl as etl

class Extractor(etl.Extractor):
    def run(self):
        """
        Create temporary table which is a full structure copy of the
        original data source table. Extract to that temporary table
        necessary data from original. Give the access to that temporary
        table to the pipeline.
        """
        self.pipeline.medium.insert()
        pass
