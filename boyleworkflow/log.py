from __future__ import annotations
from uuid import uuid4
from boyleworkflow.frozendict import FrozenDict
from typing import NewType, Protocol
from dataclasses import dataclass, field
from boyleworkflow.tree import Path, Tree
from boyleworkflow.calc import Calc, CalcOut


class NotFound(Exception):
    pass


def create_uuid_hex_str() -> str:
    return uuid4().hex


RunId = NewType("RunId", str)


def generate_run_id() -> RunId:
    return RunId(create_uuid_hex_str())


@dataclass(frozen=True)
class Run:
    run_id: str = field(init=False, default_factory=create_uuid_hex_str)
    calc: Calc
    results: FrozenDict[Path, Tree]


class CacheLog(Protocol):
    def save_run(self, run: Run):
        ...

    def recall_result(self, calc_out: CalcOut) -> Tree:
        ...


@dataclass
class Log:
    def __post_init__(self):
        self._results = {}

    def save_run(self, run: Run):
        if set(run.results) != set(run.calc.out):
            raise ValueError(f"mismatch {set(run.results)} and {set(run.calc.out)}")
        for path, result in run.results.items():
            calc_out = CalcOut(run.calc.inp, run.calc.op, path)
            self._results[calc_out] = result

    def recall_result(self, calc_out: CalcOut):
        if calc_out not in self._results:
            raise NotFound(calc_out)
        return self._results[calc_out]
