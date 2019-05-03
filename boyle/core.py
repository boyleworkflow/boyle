from typing import Mapping, Union, Any, Iterable
from pathlib import Path
import functools
import json
import hashlib

import attr

from boyle.storage import Storage


Digest = str
Loc = str
DigestMap = Mapping[Loc, Digest]
PathLike = Union[Path, str]


def unique_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True)


digest_func = hashlib.sha1

def digest_str(s: str) -> Digest:
    return digest_func(s.encode('utf-8')).hexdigest()


_CHUNK_SIZE = 1024


def digest_file(path: PathLike) -> Digest:
    digest = digest_func()
    with open(path, "rb") as f:
        while True:
            data = f.read(_CHUNK_SIZE)
            if not data:
                break
            digest.update(data)
        return Digest(digest.hexdigest())


class ConflictException(Exception):
    pass

def id_property(func):

    @property
    @functools.wraps(func)
    def id_func(self):
        id_obj = func(self)
        try:
            json = unique_json(id_obj)
        except TypeError as e:
            msg = f'The id_obj of {self} is not JSON serializable: {id_obj}'
            raise TypeError(msg) from e
        id_obj = {
            'type': type(self).__qualname__,
            'id_obj': id_obj
            }
        return digest_str(json)

    return id_func

@attr.s(auto_attribs=True, frozen=True, cmp=False)
class Op:
    cmd: str
    out_locs: Iterable[Loc]

    def __hash__(self):
        return hash(self.op_id)

    @id_property
    def op_id(self):
        return {'cmd': self.cmd, 'out_locs': self.out_locs}

    def run(self, inputs: DigestMap, storage: Storage) -> DigestMap:
        # proc = subprocess.Popen(self.cmd, cwd=work_dir, shell=True)
        # proc.wait()
        # return
        # raise NotImplemented
        return {loc: eval(self.cmd) for loc in self.out_locs}


@attr.s(auto_attribs=True, frozen=True, cmp=False)
class Calc:
    op: Op
    inputs: Mapping[Loc, Digest]

    def __hash__(self):
        return hash(self.calc_id)

    @id_property
    def calc_id(self):
        return {
            'op': self.op.op_id,
            'inputs': self.inputs,
        }

    # def __attrs_post_init__(self):
    #     assert set(self.task.inp_locs) == set(self.inputs)


@attr.s(auto_attribs=True, frozen=True, cmp=False)
class Comp:
    op: Op
    inputs: Mapping[Loc, 'Comp']
    out_loc: Loc

    def __hash__(self):
        return hash(self.comp_id)

    @id_property
    def comp_id(self):
        return {
            'op': self.op.op_id,
            'inputs': {loc: comp.comp_id for loc, comp in self.inputs.items()},
            'out_loc': self.out_loc,
        }

    def __attrs_post_init__(self):
        # assert set(self.op.inp_locs) == set(self.inputs)
        assert self.out_loc in self.op.out_locs
