from typing import Mapping, Union, Any, Iterable, NewType, Sequence, Tuple, List
import itertools

import attr

from boyleworkflow.util import id_property
from boyleworkflow.storage import Storage, Digest
from boyleworkflow.loc import Loc, check_valid_loc, normalize_loc


@attr.s(auto_attribs=True, frozen=True)
class Result:
    loc: Loc = attr.ib()
    digest: Digest

    @loc.validator
    def validate(instance, attribute, value):
        check_valid_loc(value)


class Op:
    definition: str
    op_id: str

    def run(
        self,
        inputs: Iterable[Result],
        out_locs: Iterable[Loc],
        storage: Storage,
    ) -> Iterable[Result]:
        raise NotImplemented


def _make_tuple_sorted_by_loc(items) -> Tuple:
    items = sorted(items, key=lambda x: x.loc)
    return tuple(items)


def _validate_input_locs(items):
    for item in items:
        check_valid_loc(item.loc)

    seen_locs = set()
    for item in items:
        if item.loc in seen_locs:
            raise ValueError(f"multiple definitions of loc '{loc}'")


@attr.s(auto_attribs=True, frozen=True)
class Calc:
    op: Op
    inputs: Tuple[Result, ...] = attr.ib(converter=_make_tuple_sorted_by_loc)

    @inputs.validator
    def validate(instance, attribute, value):
        _validate_input_locs(value)

    @id_property
    def calc_id(self):
        value = attr.asdict(self)
        value["inputs"] = dict(value["inputs"])
        return value


def validate_out_loc(instance, attribute, value):
    check_valid_loc(value)


@attr.s(auto_attribs=True, frozen=True)
class Comp:
    op: Op
    parents: Tuple["Comp", ...] = attr.ib(converter=_make_tuple_sorted_by_loc)
    loc: Loc = attr.ib(converter=normalize_loc)

    @parents.validator
    def validate(instance, attribute, value):
        _validate_input_locs(value)

    @loc.validator
    def validate(instance, attribute, value):
        check_valid_loc(value)

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
