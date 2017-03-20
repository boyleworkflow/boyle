import subprocess
import attr
import boyle
import boyle.core

@attr.s
class Shell:

    cmd = attr.ib()

    @boyle.core.id_property
    def task_id(self):
        return {'cmd': self.cmd}

    def run(self, work_dir):
        proc = subprocess.Popen(self.cmd, cwd=work_dir, shell=True)
        proc.wait()
