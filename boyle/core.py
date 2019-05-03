from typing import Mapping, Union, Any, Iterable, NewType, Sequence, Tuple, List
from pathlib import Path
import functools
import json
import hashlib
import itertools

import attr
import immutables  # type: ignore

Digest = NewType("Digest", str)
Loc = NewType("Loc", str)
PathLike = Union[Path, str]


digest_func = hashlib.sha1


def digest_str(s: str) -> Digest:
    return Digest(digest_func(s.encode("utf-8")).hexdigest())


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


def unique_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True)


def id_property(func):
    @property
    @functools.wraps(func)
    def id_func(self):
        try:
            return self._id_str
        except AttributeError:
            pass

        id_obj = func(self)
        try:
            json = unique_json(id_obj)
        except TypeError as e:
            msg = f"The id_obj of {self} is not JSON serializable: {id_obj}"
            raise TypeError(msg) from e
        id_obj = {"type": type(self).__qualname__, "id_obj": id_obj}
        id_str = digest_str(json)
        object.__setattr__(self, "_id_str", id_str)
        return id_str

    return id_func


def make_sorted_tuple(value: Sequence) -> Tuple:
    return tuple(sorted(value))


def _transform_obj_attr(obj, attr_name, func):
    old_value = getattr(obj, attr_name)
    new_value = func(old_value)
    object.__setattr__(obj, attr_name, new_value)


@attr.s(auto_attribs=True, frozen=True)
class Op:
    cmd: str

    @id_property
    def op_id(self):
        return attr.asdict(self)


@attr.s(auto_attribs=True, frozen=True)
class Calc:
    op: Op
    inputs: Mapping[Loc, Digest]

    def __attrs_post_init__(self):
        _transform_obj_attr(self, "inputs", immutables.Map)

    @id_property
    def calc_id(self):
        value = attr.asdict(self)
        value["inputs"] = dict(value["inputs"])
        return value


@attr.s(auto_attribs=True, frozen=True)
class Comp:
    op: Op
    inputs: Mapping[Loc, "Comp"]
    out_loc: Loc

    def __attrs_post_init__(self):
        _transform_obj_attr(self, "inputs", immutables.Map)

    @id_property
    def comp_id(self):
        return {
            "op_id": self.op.op_id,
            "input_ids": {
                loc: input_comp.comp_id
                for loc, input_comp in self.inputs.items()
            },
            "out_loc": self.out_loc,
        }



def get_parents(comps: Iterable[Comp]) -> Iterable[Comp]:
    return list(itertools.chain(*(comp.inputs.values() for comp in comps)))


def get_upstream_sorted(requested: Iterable[Comp]) -> Sequence[Comp]:
    chunks: List[Iterable[Comp]] = []
    new: Iterable[Comp] = list(requested)
    while new:
        chunks.insert(0, new)
        new = get_parents(new)
    return list(itertools.chain(*chunks))
