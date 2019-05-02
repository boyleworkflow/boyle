from typing import Iterable, Sequence, Mapping, Callable, List, Set, Optional

from typing_extensions import Protocol

import datetime


DigestMap = Mapping[Loc, Digest]


class Loc(Protocol):
    ...


class Digest(Protocol):
    ...


class Op(Protocol):
    def run(self, inputs: DigestMap, storage: Storage) -> DigestMap:
        ...


class Calc(Protocol):
    op: Op
    inputs: DigestMap


class Comp(Protocol):
    op: Op
    inputs: Mapping[Loc, Comp]
    out_loc: Loc


class Log(Protocol):
    def get_result(self, calc: Calc, out_loc: Loc) -> Digest:
        ...

    def get_calc(self, comp: Comp) -> Calc:
        ...

    def save_response(
        self, comp: Comp, digest: Digest, time: datetime.datetime
    ):
        ...

    def save_run(
        self,
        calc: Calc,
        results: DigestMap,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ):
        ...


class Storage(Protocol):
    def can_restore(self, digest: Digest) -> bool:
        ...
