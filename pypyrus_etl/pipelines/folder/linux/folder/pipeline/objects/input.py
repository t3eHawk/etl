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

        # thread = threading.current_thread()
        # log.sys.debug(f'File {source} processed by <{thread.name}>.')

        ssh = pipeline.target.ssh
        config = pipeline.config

        copy = config['copy']
        sudo = config['sudo']

        dest_folder = pipeline.mediator.path or pipeline.output.path
        filename = os.path.basename(source)
        dest = f'{dest_folder}/{filename}'

        if copy is True:
            log.sys.info(f'Copy <{source}> to <{dest}>.')
            cmd = f'cp {source} {dest}'
        else:
            log.sys.info(f'Move <{source}> to <{dest}>.')
            cmd = f'mv {source} {dest}'

        exec_kwargs = {}

        if sudo is True:
            cmd = f'sudo {cmd}'
            exec_kwargs['get_pty'] = True

        stdin, stdout, stderr = ssh.exec_command(cmd, **exec_kwargs)
        stdout = stdout.read()
        stderr = stderr.read()

        if stdout:
            log.sys.info(stdout.decode())
        if stderr:
            log.sys.error(stderr.decode())
        pass
