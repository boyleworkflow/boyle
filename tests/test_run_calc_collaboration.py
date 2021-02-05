from boyleworkflow.loc import Loc
from boyleworkflow.tree import Tree
import pytest
from unittest.mock import Mock, call
from boyleworkflow.calc import run_calc, Calc
from tests.util import tree_from_dict

CALC = Calc(
    tree_from_dict({name: f"result:{name}" for name in ["i1", "i2", "i3"]}),
    "op",
    frozenset(
        [
            Loc("o1"),
            Loc("o2"),
        ]
    ),
)


def test_creates_exactly_one_sandbox():
    env = Mock()
    run_calc(CALC, env)
    env.create_sandbox.assert_called_once()  # type: ignore


def test_runs_op_in_sandbox():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run_calc(CALC, env)
    env.run_op.assert_called_with(CALC.op, sandbox)  # type: ignore


def test_places_inputs_in_sandbox():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run_calc(CALC, env)
    env.place.assert_called_once_with(sandbox, CALC.inp)  # type: ignore


def test_destroys_sandbox_after_finishing():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run_calc(CALC, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)  # type: ignore


def test_destroys_sandbox_after_failed_run():
    env = Mock(
        run_op=Mock(side_effect=Exception()),
    )
    sandbox = env.create_sandbox()  # type: ignore
    with pytest.raises(Exception):
        run_calc(CALC, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)  # type: ignore


def test_asks_env_to_stow_out_locs():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run_calc(CALC, env)
    assert env.stow.call_args_list == [  # type:ignore
        call(sandbox, loc) for loc in CALC.out  # type:ignore
    ]


def test_returns_stowed_results():
    expected_results = {loc: Tree({}, f"digest:{loc}") for loc in CALC.out}
    env = Mock(stow=lambda sandbox, loc: expected_results[loc])  # type: ignore
    results = run_calc(CALC, env)
    assert results == expected_results
