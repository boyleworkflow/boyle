from typing import Any, Mapping, NewType
from dataclasses import dataclass
import datetime
from boyleworkflow.trees import Tree

Glob = NewType("Glob", str)


Op = Mapping[str, Any]


@dataclass
class Calc:
    inp: Tree
    op: Op


@dataclass
class Run:
    run_id: str
    calc: Calc
    start_time: datetime.datetime
    end_time: datetime.datetime
    results: Mapping[Glob, Tree]
