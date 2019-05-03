from typing import Iterable, Mapping, cast
import os
import tempfile
import subprocess
from pathlib import Path

from boyleworkflow.core import Calc, Op, Loc, Digest, PathLike, is_valid_loc
from boyleworkflow.log import Log
from boyleworkflow.storage import Storage


class RunError(Exception):
    pass


WORK_BASE_DIR = 'work_dir'
STDERR_PATH = 'stderr'
STDOUT_PATH = 'stdout'

def is_inside(path, parent):
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


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

    with tempfile.TemporaryDirectory() as td:
        container_dir = Path(td).resolve()

        file_dir = container_dir / WORK_BASE_DIR
        work_dir = file_dir / op.work_dir

        assert is_inside(work_dir, file_dir), (work_dir, file_dir)

        work_dir.mkdir(parents=True)

        fill_run_dir(calc, file_dir, storage)

        devnull = cast(PathLike, os.devnull)
        stdout_path = container_dir / STDOUT_PATH if op.stdout else devnull
        stderr_path = container_dir / STDERR_PATH if op.stderr else devnull

        with open(stdout_path, 'wb') as stdout, open(stderr_path, 'wb') as stderr:
            proc = subprocess.run(
                op.cmd,
                cwd=work_dir,
                shell=op.shell,
                stdout=stdout,
                stderr=stderr,
                )

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
