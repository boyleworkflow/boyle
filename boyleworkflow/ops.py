from typing import Iterable, Mapping, cast
import os
import tempfile
import subprocess
from pathlib import Path

import attr

from boyleworkflow.core import Calc, Op, Result
from boyleworkflow.loc import Loc, is_valid_loc, SpecialFilePath, BASE_DIR
from boyleworkflow.storage import Digest
from boyleworkflow.util import PathLike, id_property, unique_json
from boyleworkflow.log import Log
from boyleworkflow.storage import Storage


class RunError(Exception):
    pass


_SPECIAL_FILE_MODES = {
    SpecialFilePath.STDIN: "rb",
    SpecialFilePath.STDOUT: "wb",
    SpecialFilePath.STDERR: "wb",
}


def is_inside(path, parent):
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def place_inputs(inputs: Iterable[Result], the_dir: PathLike, storage: Storage):
    the_dir = Path(the_dir).resolve()
    contents = list(the_dir.iterdir())
    assert not contents, contents

    for inp in inputs:
        dst_path = (the_dir / inp.loc).resolve()
        assert is_valid_loc(inp.loc), f"invalid loc {inp.loc}"
        storage.restore(inp.digest, dst_path)


@attr.s(auto_attribs=True, frozen=True)
class ShellOp(Op):
    cmd: str
    shell: bool = False
    stdin: bool = False
    stderr: bool = True
    stdout: bool = True
    work_dir: str = "."

    @property
    def definition(self):
        return unique_json(attr.asdict(self))

    @id_property
    def op_id(self):
        return attr.asdict(self)

    def run(
        self,
        inputs: Iterable[Result],
        out_locs: Iterable[Loc],
        storage: Storage,
    ) -> Iterable[Result]:

        with tempfile.TemporaryDirectory() as td:
            container_dir = Path(td).resolve()

            base_dir = container_dir / BASE_DIR
            work_dir = base_dir / self.work_dir

            assert is_inside(work_dir, base_dir), (work_dir, base_dir)

            work_dir.mkdir(parents=True)

            place_inputs(inputs, base_dir, storage)

            devnull = cast(PathLike, os.devnull)

            def open_special_file(file, activated):
                if activated:
                    path = (base_dir / file.value).resolve()
                    assert is_inside(path, container_dir), (path, container_dir)
                    assert not is_inside(path, base_dir), (path, base_dir)
                else:
                    path = devnull

                return open(path, _SPECIAL_FILE_MODES[file])

            special_files = dict(
                stdin=open_special_file(SpecialFilePath.STDIN, self.stdin),
                stdout=open_special_file(SpecialFilePath.STDOUT, self.stdout),
                stderr=open_special_file(SpecialFilePath.STDERR, self.stderr),
            )

            try:
                proc = subprocess.run(
                    self.cmd, cwd=work_dir, shell=self.shell, **special_files
                )
            finally:
                for file in special_files.values():
                    file.close()

            try:
                proc.check_returncode()
            except subprocess.CalledProcessError as e:
                info = {"op": self, "inputs": inputs, "message": str(e)}
                raise RunError(info) from e

            return [
                Result(loc, storage.store(os.path.join(work_dir, loc)))
                for loc in out_locs
            ]
