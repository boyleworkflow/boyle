from typing import Iterable, Mapping
import os
import tempfile
import subprocess

from boyle.core import Calc, Op, Loc, Digest
from boyle.log import Log
from boyle.storage import Storage


class RunError(Exception):
    pass


def run(calc: Calc, out_locs: Iterable[Loc], storage: Storage) -> Mapping[Loc, Digest]:
    op = calc.op

    with tempfile.TemporaryDirectory() as work_dir:
        for loc, digest in calc.inputs.items():
            storage.restore(digest, os.path.join(work_dir, loc))

        proc = subprocess.run(op.cmd, cwd=work_dir, shell=True)
        try:
            proc.check_returncode()
        except subprocess.CalledProcessError as e:
            info = {
                'calc': calc,
                'message': str(e),
            }
            raise RunError(info) from e

        return {
            loc: storage.store(os.path.join(work_dir, loc))
            for loc in out_locs
            }
