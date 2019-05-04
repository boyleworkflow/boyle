from typing import Mapping, Union, Any, Iterable, NewType, Sequence, Tuple, List
from pathlib import Path, PurePath
import functools
import json
import hashlib
import itertools
from enum import Enum

import attr


BASE_DIR = "files"


class SpecialFilePath(Enum):
    STDIN = "../stdin"
    STDOUT = "../stdout"
    STDERR = "../stderr"


Digest = NewType("Digest", str)
Loc = NewType("Loc", str)
PathLike = Union[Path, str]


_SPECIAL_ALLOWED_LOCS = {f.value for f in SpecialFilePath}


def check_valid_loc(s: str):
    if s in _SPECIAL_ALLOWED_LOCS:
        return

    if s == "":
        raise ValueError(f"empty loc '{s}' is not allowed (try '.' instead)")

    p = PurePath(s)

    if p.is_absolute():
        raise ValueError(f"loc '{p}' is absolute")

    if p.is_reserved():
        raise ValueError(f"loc '{p}' is reserved.")

    if ".." in p.parts:
        raise ValueError(f"loc '{p}' contains disallowed '..'")


def check_valid_input_loc(s: str):
    check_valid_loc(s)

    if s in [SpecialFilePath.STDOUT.value, SpecialFilePath.STDERR.value]:
        raise ValueError(f"loc '{s}' cannot be used as input (try renaming it)")


def check_valid_output_loc(s: str):
    check_valid_loc(s)

    if s in [SpecialFilePath.STDIN.value]:
        raise ValueError(
            f"loc '{s}' cannot be used as output (try renaming it)"
        )


def normalize_loc(s: str) -> Loc:
    check_valid_loc(s)
    path = Path(s)
    return Loc(str(Path(*path.parts)))


def is_valid_loc(s: str) -> bool:
    try:
        check_valid_loc(s)
        return True
    except ValueError:
        return False


def _attrs_loc_validator(instance, attribute, value):
    check_valid_loc(value)


def _attrs_loc_input_validator(instance, attribute, value):
    check_valid_input_loc(value)


def _attrs_loc_output_validator(instance, attribute, value):
    check_valid_output_loc(value)


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


@attr.s(auto_attribs=True, frozen=True)
class Result:
    loc: Loc = attr.ib(validator=_attrs_loc_validator)
    digest: Digest


def _make_tuple_sorted_by_loc(items):
    items = sorted(items, key=lambda x: x.loc)
    return tuple(items)


def _check_unique_locs(items):
    seen_locs = set()
    for item in items:
        if item.loc in seen_locs:
            raise ValueError(f"multiple definitions of loc '{loc}'")


def _input_locs_validator(instance, attribute, value):
    for item in value:
        check_valid_input_loc(item.loc)

    _check_unique_locs(value)


@attr.s(auto_attribs=True, frozen=True)
class Calc:
    op: Op
    inputs: Tuple[Result, ...] = attr.ib(
        validator=_input_locs_validator, converter=_make_tuple_sorted_by_loc
    )

    @id_property
    def calc_id(self):
        value = attr.asdict(self)
        value["inputs"] = dict(value["inputs"])
        return value


@attr.s(auto_attribs=True, frozen=True)
class Comp:
    op: Op
    parents: Tuple["Comp", ...] = attr.ib(
        validator=_input_locs_validator, converter=_make_tuple_sorted_by_loc
    )
    loc: Loc = attr.ib(
        validator=_attrs_loc_output_validator, converter=normalize_loc
    )

    @id_property
    def comp_id(self):
        return {
            "op_id": self.op.op_id,
            "input_ids": [parent.comp_id for parent in self.parents],
            "loc": self.loc,
        }


def get_parents(comps: Iterable[Comp]) -> Iterable[Comp]:
    return list(itertools.chain(*(comp.parents for comp in comps)))


def get_upstream_sorted(requested: Iterable[Comp]) -> Sequence[Comp]:
    chunks: List[Iterable[Comp]] = []
    new: Iterable[Comp] = list(requested)
    while new:
        chunks.insert(0, new)
        new = get_parents(new)
    return list(itertools.chain(*chunks))
