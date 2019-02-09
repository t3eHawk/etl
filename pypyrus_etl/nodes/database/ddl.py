from sqlalchemy.schema import DDLElement

class alter(DDLElement):
    def __init__(self, object, action, *args, **kwargs):
        self.object = object
        self.action = action
        self.args = args
        self.kwargs = kwargs
        pass
