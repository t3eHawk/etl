import paramiko

class Host():
    """That class represents a host object."""
    def __init__(
        self, name, system=None, host=None, port=None, user=None, password=None,
        key=None, config=None, ssh=True, sftp=True
    ):
        self.name = name

        if config is not None:
            pass

        if ssh is True:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(
                host, port=port, username=user, password=password,
                key_filename=key)
        else:
            self.ssh = None

        if sftp is True:
            if ssh is True:
                self.sftp = self.ssh.open_sftp()
            elif ssh is False:
                transport = paramiko.Transport((host, port))
                transport.connect(username=user, password=password)
                self.sftp = paramiko.SFTPClient.from_transport(transport)
        else:
            sftp = None
        pass

    def move(self, src, dest):
        if self.sftp is not None:
            self.sftp.rename(src, dest)
        elif ssh is not None:
            command = f'mv {src} {dest}'
            stdin, stdout, stderr = self.ssh.exec_command(command)
        pass
