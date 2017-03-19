import subprocess
import attr
import boyle

@attr.s
class Shell:

    cmd = attr.ib()

    @boyle.id_property
    def __id__(self):
        return {'cmd': self.cmd}

    def run(self, work_dir):
        proc = subprocess.Popen(self.cmd, cwd=work_dir, shell=True)
        proc.wait()
