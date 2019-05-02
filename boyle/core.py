from typing import Mapping, Sequence, Union, Any
from pathlib import Path
import functools
import json
import hashlib

import attr

PathLike = Union[Path, str]

digest_func = hashlib.sha1


def unique_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True)


def digest_str(s: str) -> Digest:
    return digest_func(s.encode('utf-8')).hexdigest()


def digest_file(path: PathLike) -> Digest:
    with open(path, 'rb') as f:
        return digest_func(f.read()).hexdigest()


class NotFoundException(Exception):
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


Environment = Path
Digest = str
Loc = str

@attr.s(auto_attribs=True)
class Op:
    cmd: str

    @id_property
    def op_id(self):
        return {'cmd': self.cmd}

    def run(self, env: Environment):
        raise NotImplemented


@attr.s(auto_attribs=True)
class Calc:
    op: Op
    inputs: Mapping[Loc, Digest]

    @id_property
    def calc_id(self):
        return {
            'op': self.op.op_id,
            'inputs': self.inputs,
        }

    def __attrs_post_init__(self):
        assert set(self.task.inp_locs) == set(self.inputs)


@attr.s(auto_attribs=True)
class Comp:
    op: Op
    inputs: Mapping[Loc, Comp]
    out_loc: Loc

    @id_property
    def comp_id(self):
        return {
            'op': self.op.op_id,
            'inputs': {loc: comp.comp_id for loc, comp in self.inputs.items()},
            'out_loc': self.out_loc,
        }

    def __attrs_post_init__(self):
        assert set(self.task.inp_locs) == set(self.inputs)
        assert self.out_loc in task.out_locs
