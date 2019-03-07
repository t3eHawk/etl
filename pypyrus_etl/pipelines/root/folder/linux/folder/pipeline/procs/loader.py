import pypyrus_etl as etl

class Loader(etl.Loader):
    def run(self):
        pipeline = self.pipeline
        config = pipeline.config
        input = pipeline.input
        output = pipeline.output

        purge = config['purge']
        if purge is True:
            output.purge()
        input.push_files()
        # output.check_files()
        pass
