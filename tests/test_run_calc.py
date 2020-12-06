import pytest
from pytest import fixture
from unittest.mock import Mock, call
from boyleworkflow.calc import run, Calc, Loc, Result


@fixture
def calc():
    return Calc(
        {Loc(name): Result(f"result:{name}") for name in ["i1", "i2", "i3"]},
        Mock(),
        [
            Loc("o1"),
            Loc("o2"),
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
    expected_calls = [
        call(sandbox, loc, digest) for loc, digest in calc.inp.items()
    ]
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
    expected_calls = [call(sandbox, loc) for loc in calc.out]
    assert expected_calls == env.stow.call_args_list


def test_returns_digests(calc):
    expected_results = {loc: f"digest:{loc}" for loc in calc.out}
    env = Mock(stow=lambda sandbox, loc: expected_results[loc])
    results = run(calc, env)
    assert results == expected_results
