from typing import Iterable, Mapping
import os
import tempfile
import subprocess
from pathlib import Path

from boyle.core import Calc, Op, Loc, Digest, PathLike, is_valid_loc
from boyle.log import Log
from boyle.storage import Storage


class RunError(Exception):
    pass


def fill_run_dir(calc: Calc, the_dir: PathLike, storage: Storage):
    the_dir = Path(the_dir).resolve()
    contents = list(the_dir.iterdir())
    assert not contents, contents

    for loc, digest in calc.inputs.items():
        dst_path = (the_dir / loc).resolve()
        assert is_valid_loc(loc), f'invalid loc {loc}'
        storage.restore(digest, dst_path)


def run(calc: Calc, out_locs: Iterable[Loc], storage: Storage) -> Mapping[Loc, Digest]:
    op = calc.op

    with tempfile.TemporaryDirectory() as work_dir:
        fill_run_dir(calc, work_dir, storage)

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
