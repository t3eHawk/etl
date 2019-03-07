from pypyrus_etl.nodes.host import Host
from pypyrus_etl.nodes.link import Link
from pypyrus_etl.nodes.database import Database

from pypyrus_etl.objects.table import Table
from pypyrus_etl.objects.folder import Folder

class Pipeline():
    def __new__(
        self, name, source, object, target, config,
        run_timestamp=None, log=None, job=None
    ):
        # ETL of objects from one DB to another using dblink.
        if isinstance(source, Link) is True:
            if isinstance(target, Database) is True:
                if isinstance(object, Table) is True:
                    from .root.table.dblink.table.pipeline import Pipeline
        if isinstance(source, Host) is True:
            if isinstance(target, Host) is True:
                if isinstance(object, Folder) is True:
                    if id(source) == id(target):
                        from .root.folder.linux.folder.pipeline import Pipeline
                    else:
                        from .root.folder.sftp.folder.pipeline import Pipeline
        return Pipeline(
            name, source, object, target, config,
            run_timestamp=run_timestamp, log=log, job=job)

# class Pipelines():
#     def __new__(self, source, target, folder, log=None):
#         # ETL of objects from one DB to another using dblink.
#         if isinstance(source, etl.Link) is True:
#             if isinstance(target, etl.Database) is True:
#                 return etl.pipeline.dblink.Pipelines(
#                     source, target, folder, log=log)
