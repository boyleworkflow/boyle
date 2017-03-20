import os
import shutil
import attr
import boyle
import boyle.core

@attr.s
class File:

    path = attr.ib()

    @boyle.core.id_property
    def instr_id(self):
        return {'path': self.path}

    def digest(self, work_dir):
        path = os.path.join(work_dir, self.path)
        with open(path, 'rb') as f:
            return boyle.digest_func(f.read()).hexdigest()

    def copy(self, src_dir, dst_dir):
        src_path = os.path.join(src_dir, self.path)
        dst_path = os.path.join(dst_dir, self.path)
        shutil.copy2(src_path, dst_path)
