from boyleworkflow.tree import Leaf, Tree
import pytest
from pytest import fixture
from unittest.mock import Mock, call
from boyleworkflow.calc import run, Calc, Path
from tests.util import tree_from_dict


@fixture
def calc():
    return Calc(
        tree_from_dict({name: f"result:{name}" for name in ["i1", "i2", "i3"]}),
        Mock(),
        [
            Path.from_string("o1"),
            Path.from_string("o2"),
        ],
    )


def test_creates_exactly_one_sandbox(calc):
    env = Mock()
    run(calc, env)
    env.create_sandbox.assert_called_once()


def test_runs_op_in_sandbox(calc):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc, env)
    env.run_op.assert_called_with(calc.op, sandbox)


def test_places_inputs_in_sandbox(calc):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc, env)
    env.place.assert_called_once_with(sandbox, calc.inp)


def test_destroys_sandbox_after_finishing(calc):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)


def test_destroys_sandbox_after_failed_run(calc):
    env = Mock(
        run_op=Mock(side_effect=Exception()),
    )
    sandbox = env.create_sandbox()
    with pytest.raises(Exception):
        run(calc, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)


def test_asks_env_to_stow_out_paths(calc):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc, env)
    assert env.stow.call_args_list == [call(sandbox, path) for path in calc.out]


def test_returns_tree_representing_results(calc):
    calc_results = {path: Leaf(f"digest:{path}") for path in calc.out}
    expected_result_tree = Tree.from_nested_items(calc_results)
    env = Mock(stow=lambda sandbox, path: calc_results[path])
    result_tree = run(calc, env)
    assert result_tree == expected_result_tree
