import pytest
from boyleworkflow.calc import CalcOut
from boyleworkflow.tree import Tree, Path
from boyleworkflow.log import Log, CacheLog, NotFound



def create_log() -> CacheLog:
    return Log()


def test_can_retrieve_stored_result():
    calc_out = CalcOut(Tree({}), "op", Path.from_string("out"))
    result = Tree({})
    log = create_log()
    log.save_result(calc_out, result)
    assert log.recall_result(calc_out) == result


def test_raises_exception_if_not_found():
    calc_out = CalcOut(Tree({}), "op", Path.from_string("out"))
    log = create_log()
    with pytest.raises(NotFound):
        log.recall_result(calc_out)
