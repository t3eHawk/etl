import os
import time
import queue
import threading

from pypyrus_etl.objects.folder import Folder

class Input(Folder):
    def prepare(self):
        pass

    def select_files(self):
        pipeline = self.pipeline
        log = pipeline.log
        parser = pipeline.parser
        log.sys.info('Prepare list of files.')
        files = parser.parse_files()
        count = len(files)
        self.files = files
        log.sys.info(f'Done. Found files <{count}>.')
        pass

    def push_files(self):
        pipeline = self.pipeline
        log = pipeline.log
        config = pipeline.config
        threads = config['threads'] or 1
        self.queue = queue.Queue()
        for file in self.files:
            self.queue.put(file['path'])
        for i in range(threads):
            thread = threading.Thread(target=self.process_queue, daemon=True)
            thread.start()
        self.queue.join()
        pass

    def process_queue(self):
        while True:
            source = self.queue.get()
            self.push_file(source)
            self.queue.task_done()
        pass

    def push_file(self, source):
        pipeline = self.pipeline
        log = pipeline.log

        s_sftp = pipeline.source.sftp

        config = pipeline.config

        dest_folder = pipeline.mediator.path or pipeline.output.path
        filename = os.path.basename(source)
        dest = f'{dest_folder}/{filename}'

        log.sys.info(f'Copy <{source}> to <{dest}>.')
        if pipeline.mediator.path is not None:
            s_sftp.get(source, dest)
        else:
            t_sftp = pipeline.target.sftp
            with t_sftp.open(dest, mode='w') as fh:
                s_sftp.getfo(source, fh)
        pass

    def purge(self):
        s_sftp = self.pipeline.source.sftp
        log = self.pipeline.log
        path = self.path
        log.sys.info(f'All files in <{path}> will be deleted.')
        for file in self.files:
            filepath = file['path']
            s_sftp.remove(filepath)
            log.sys.info(f'File <{filepath}> removed.')
        pass
