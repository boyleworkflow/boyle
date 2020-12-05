import pytest
from pytest import fixture
from unittest.mock import Mock, call
from boyleworkflow import run, Calc


@fixture
def calc():
    return Calc(["i1", "i2", "i3"], Mock(), ["o1", "o2"])


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
    expected_calls = [call(item, sandbox) for item in calc.inp]
    assert expected_calls == env.place.call_args_list


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


def test_stows_results(calc):
    env = Mock()
    sandbox = env.create_sandbox()
    run(calc, env)
    expected_calls = [call(loc, sandbox) for loc in calc.out]
    assert expected_calls == env.stow.call_args_list