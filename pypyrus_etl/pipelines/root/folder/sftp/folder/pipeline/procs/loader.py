import pypyrus_etl as etl

class Loader(etl.Loader):
    def run(self):
        pipeline = self.pipeline
        config = pipeline.config
        input = pipeline.input
        mediator = pipeline.mediator
        output = pipeline.output

        if config['purge'] is True:
            output.purge()

        if mediator.path is not None:
            mediator.push_files()
        # output.check_files()

        if config['cut'] is True:
            input.purge()

        output.check_files()
        pass
