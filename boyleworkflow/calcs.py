from typing import Mapping, NewType, Protocol
from dataclasses import dataclass
import datetime
from boyleworkflow.trees import Tree

Glob = NewType("Glob", str)


class Op(Protocol):
    pass


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
