from pypyrus_etl.objects.folder import Folder

class Mediator(Folder):
    def prepare(self):
        """Prepare object for pipeline run."""
        path = self.path
        if path is not None:
            sftp = self.pipeline.target.sftp
            log = self.pipeline.log
            length = len(path)
            for i in range(length):
                # Create folder if it is not exists.
                if (i != 0 and path[i] == '/') or i == length - 1:
                    current = path[:i + 1]
                    try:
                        sftp.stat(current)
                    except FileNotFoundError:
                        sftp.mkdir(current)
        pass
