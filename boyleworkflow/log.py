from __future__ import annotations
from typing import Protocol
from dataclasses import dataclass
from boyleworkflow.tree import Tree
from boyleworkflow.calc import CalcOut


class NotFound(Exception):
    pass


class CacheLog(Protocol):
    def save_result(self, calc_out: CalcOut, result: Tree):
        ...

    def recall_result(self, calc_out: CalcOut) -> Tree:
        ...


@dataclass
class Log:
    def __post_init__(self):
        self._results = {}

    def save_result(self, calc_out: CalcOut, result: Tree):
        self._results[calc_out] = result

    def recall_result(self, calc_out: CalcOut):
        if calc_out not in self._results:
            raise NotFound(calc_out)
        return self._results[calc_out]
