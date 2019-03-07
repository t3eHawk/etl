from pypyrus_etl.objects.folder import Folder

class Output(Folder):
    def prepare(self):
        """Prepare object for pipeline run."""
        path = self.path
        pipeline = self.pipeline
        config = pipeline.config
        autocreate = config['autocreate']
        sftp = pipeline.target.sftp
        log = pipeline.log
        length = len(path)
        for i in range(length):
            # Create folder if it is not exists and if it is allowed
            # by configurator.
            if (i != 0 and path[i] == '/') or i == length - 1:
                current = path[:i + 1]
                try:
                    sftp.stat(current)
                except FileNotFoundError:
                    if autocreate is True:
                        sftp.mkdir(current)
                        log.sys.info(f'File <{current}> created.')
                    else:
                        raise FileNotFoundError(
                            f'File <{current}> not found!')
        pass

    def purge(self):
        path = self.path
        pipeline = self.pipeline
        sftp = pipeline.target.sftp
        log = pipeline.log
        log.sys.info(f'All files in <{path}> will be deleted.')
        for file in sftp.listdir(path):
            filepath = f'{path}/{file}'
            sftp.remove(filepath)
            log.sys.info(f'File <{filepath}> removed.')
        pass

    def check_files(self):
        path = self.path
        pipeline = self.pipeline
        log = pipeline.log
        sftp = pipeline.target.sftp
        input = pipeline.input
        files = input.files
        log.sys.info('Now analyze checksums...')
        for file in files:
            filename = file['name']
            filesize = file['size']
            dest = f'{self.path}/{filename}'
            size = sftp.stat(dest).st_size
            log.sys.info(
                f'File {filename}.'\
                f'Input size {filesize}.'\
                f'Output size {size}.')
            if filesize != size:
                log.sys.error(f'File {filename} checksum failed!')
        pass
