from boyleworkflow.tree import Leaf
import pytest
from pytest import fixture
from unittest.mock import Mock, call
from boyleworkflow.calc import run, CalcBundle, Path
from tests.util import tree_from_dict


@fixture
def calc_bundle():
    return CalcBundle(
        tree_from_dict({name: f"result:{name}" for name in ["i1", "i2", "i3"]}),
        Mock(),
        frozenset(
            [
                Path.from_string("o1"),
                Path.from_string("o2"),
            ]
        ),
    )


def test_creates_exactly_one_sandbox(calc_bundle):
    env = Mock()
    run(calc_bundle, env)
    env.create_sandbox.assert_called_once()


def test_runs_op_in_sandbox(calc_bundle):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc_bundle, env)
    env.run_op.assert_called_with(calc_bundle.op, sandbox)


def test_places_inputs_in_sandbox(calc_bundle):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc_bundle, env)
    env.place.assert_called_once_with(sandbox, calc_bundle.inp)


def test_destroys_sandbox_after_finishing(calc_bundle):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc_bundle, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)


def test_destroys_sandbox_after_failed_run(calc_bundle):
    env = Mock(
        run_op=Mock(side_effect=Exception()),
    )
    sandbox = env.create_sandbox()
    with pytest.raises(Exception):
        run(calc_bundle, env)
    env.destroy_sandbox.assert_called_once_with(sandbox)


def test_asks_env_to_stow_out_paths(calc_bundle):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc_bundle, env)
    assert env.stow.call_args_list == [
        call(sandbox, path) for path in calc_bundle.out
    ]


def test_returns_mapping_with_results(calc_bundle):
    expected_results = {
        path: Leaf(f"digest:{path}") for path in calc_bundle.out
    }
    env = Mock(stow=lambda sandbox, path: expected_results[path])
    results = run(calc_bundle, env)
    assert results == expected_results
