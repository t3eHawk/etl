class Process():
    """This class represents the part of the ETL pipeline."""
    def __init__(self, pipeline, **kwargs):
        self.pipeline = pipeline
        pass

    def run(self, *args, **kwargs):
        """Run all tasks defined in the process."""
        pass

class Extractor(Process):
    """This class represents extraction part of the ETL pipeline."""
    pass

class Transformer(Process):
    """This class represents transformation part of the ETL pipeline."""
    pass

class Loader(Process):
    """This class represents loading part of the ETL pipeline."""
    pass
