import pytest
from boyleworkflow.loc import Loc
from boyleworkflow.tree import Tree
from boyleworkflow.log import Log, CacheLog, NotFound, Run, Calc
from tests.util import tree_from_dict


def create_log() -> CacheLog:
    return Log()


def test_can_retrieve_stored_result():
    calc = Calc(Tree({}), "op", frozenset({Loc(".")}))
    result = Tree({})
    run = Run(calc, result)
    log = create_log()
    log.save_run(run)
    assert log.recall_result(calc) == result


def test_can_retrieve_stored_result_with_complicated_tree():
    calc = Calc(Tree({}), "op", frozenset({Loc(".")}))
    result = tree_from_dict({"a": {"b1": "x", "b2": {"c": "y"}}})
    run = Run(calc, result)
    log = create_log()
    log.save_run(run)
    assert log.recall_result(calc) == result


def test_raises_exception_if_not_found():
    calc = Calc(Tree({}), "op", frozenset({Loc("out")}))
    log = create_log()
    with pytest.raises(NotFound):
        log.recall_result(calc)
