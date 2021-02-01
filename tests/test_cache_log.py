from boyleworkflow.frozendict import FrozenDict
import pytest
from boyleworkflow.calc import Calc, CalcOut
from boyleworkflow.tree import Tree, Path
from boyleworkflow.log import Log, CacheLog, NotFound, Run
from tests.util import tree_from_dict


def create_log() -> CacheLog:
    return Log()


def test_can_retrieve_stored_result():
    calc_out = CalcOut(Tree({}), "op", Path.from_string("out"))
    calc = Calc(calc_out.inp, calc_out.op, frozenset({calc_out.out}))
    result = Tree({})
    results = FrozenDict({calc_out.out: result})
    run = Run(calc, results)
    log = create_log()
    log.save_run(run)
    assert log.recall_result(calc_out) == result


def test_can_retrieve_stored_result_with_complicated_tree():
    calc_out = CalcOut(Tree({}), "op", Path.from_string("out"))
    calc = Calc(calc_out.inp, calc_out.op, frozenset({calc_out.out}))
    result = tree_from_dict({
        "a": {"b1": "x", "b2": {"c": "y"}}
    })
    results = FrozenDict({calc_out.out: result})
    run = Run(calc, results)
    log = create_log()
    log.save_run(run)
    assert log.recall_result(calc_out) == result

def test_raises_exception_if_not_found():
    calc_out = CalcOut(Tree({}), "op", Path.from_string("out"))
    log = create_log()
    with pytest.raises(NotFound):
        log.recall_result(calc_out)
