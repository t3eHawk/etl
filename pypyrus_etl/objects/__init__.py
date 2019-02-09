class Base():
    """That is a basic class for all other objects."""
    def __init__(self, name, pipeline=None):
        self.name = name.lower()
        self.pipeline = pipeline
        pass
