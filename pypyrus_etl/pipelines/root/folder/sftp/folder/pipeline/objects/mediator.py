import os
import queue
import threading

from pypyrus_etl.objects.folder import Folder

class Mediator(Folder):
    def prepare(self):
        """Prepare object for pipeline run."""
        path = self.path
        for root, dirs, files in os.walk(path):
            for file in files:
                os.remove(os.path.join(root, file))
        pass

    def push_files(self):
        pipeline = self.pipeline
        log = pipeline.log
        config = pipeline.config
        threads = config['threads'] or 1
        self.queue = queue.Queue()
        for root, dirs, files in os.walk(self.path):
            for file in files:
                self.queue.put(os.path.join(root, file))
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

        sftp = pipeline.target.sftp

        config = pipeline.config

        dest_folder = pipeline.output.path
        filename = os.path.basename(source)
        dest = f'{dest_folder}/{filename}'

        log.sys.info(f'Copy <{source}> to <{dest}>.')
        sftp.put(source, dest)

        pass
