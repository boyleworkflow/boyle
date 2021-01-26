from boyleworkflow.tree import Tree
import pytest
from unittest.mock import Mock, call
from boyleworkflow.calc import run, CalcBundle, Path
from tests.util import tree_from_dict

CALC_BUNDLE = CalcBundle(
    tree_from_dict({name: f"result:{name}" for name in ["i1", "i2", "i3"]}),
    Mock(),
    frozenset(
        [
            Path.from_string("o1"),
            Path.from_string("o2"),
        ]
    ),
)


def test_creates_exactly_one_sandbox():
    env = Mock()
    run(CALC_BUNDLE, env)
    env.create_sandbox.assert_called_once()  # type: ignore


def test_runs_op_in_sandbox():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run(CALC_BUNDLE, env)
    env.run_op.assert_called_with(CALC_BUNDLE.op, sandbox)  # type: ignore


def test_places_inputs_in_sandbox():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run(CALC_BUNDLE, env)
    env.place.assert_called_once_with(sandbox, CALC_BUNDLE.inp)  # type: ignore


def test_destroys_sandbox_after_finishing():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run(CALC_BUNDLE, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)  # type: ignore


def test_destroys_sandbox_after_failed_run():
    env = Mock(
        run_op=Mock(side_effect=Exception()),
    )
    sandbox = env.create_sandbox()  # type: ignore
    with pytest.raises(Exception):
        run(CALC_BUNDLE, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)  # type: ignore


def test_asks_env_to_stow_out_paths():
    env = Mock()
    sandbox = env.create_sandbox()  # type: ignore
    run(CALC_BUNDLE, env)
    assert env.stow.call_args_list == [  # type:ignore
        call(sandbox, path) for path in CALC_BUNDLE.out  # type:ignore
    ]


def test_returns_mapping_with_results():
    expected_results = {path: Tree({}, f"digest:{path}") for path in CALC_BUNDLE.out}
    env = Mock(stow=lambda sandbox, path: expected_results[path])  # type: ignore
    results = run(CALC_BUNDLE, env)
    assert results == expected_results
