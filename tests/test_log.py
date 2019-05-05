#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `boyleworkflow` package."""

import tempfile
import shutil
import os
import datetime

import pytest

from click.testing import CliRunner

import boyleworkflow
from boyleworkflow import cli

from boyleworkflow.core import Comp, Calc, Result
from boyleworkflow.ops import ShellOp


@pytest.fixture
def log(request):

    temp_dir = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(temp_dir)

    request.addfinalizer(fin)

    log_path = os.path.join(temp_dir, "log.db")
    log = boyleworkflow.Log(log_path)

    return log


def generate_test_data():

    dependencies = {
        ("a",): [],
        ("b",): ["a"],
        ("c", "d"): ["a", "b"],
        ("e", "f"): ["a", "b", "c"],
    }

    digests = {
        "a": "digest 1",
        "b": "digest 2",
        "c": "digest 1",
        "d": "digest 3",
        "e": "digest 3",
        "f": "digest 4",
    }

    locs = {
        "a": "loc_A",
        "b": "loc_B",
        "c": "loc_C",
        "d": "loc_D",
        "e": "loc_D",
        "f": "loc_F",
    }

    calcs = {}
    comps = {}

    all_results = []

    for out_keys in sorted(dependencies):
        op = ShellOp(f"command for {out_keys}")

        inputs = [
            Result(locs[inp_key], digests[inp_key])
            for inp_key in dependencies[out_keys]
        ]

        calc = Calc(op, inputs)
        calcs[out_keys] = calc

        parents = [comps[inp_key] for inp_key in dependencies[out_keys]]

        for out_key in out_keys:
            comp = Comp(op, parents, locs[out_key])
            comps[out_key] = comp

        results = [
            Result(locs[out_key], digests[out_key])
            for out_key in out_keys
        ]

        all_results.append(
            dict(
                calc=calc,
                comps=[comps[out_key] for out_key in out_keys],
                results=results,
            )
        )

    return all_results


def test_log_read_write_results(log):

    data = generate_test_data()

    t = datetime.datetime.utcnow()

    for d in data:
        calc = d["calc"]
        comps = d["comps"]
        results = d["results"]


        for comp in comps:
            # Since we are taking the compositions in topological order
            # the calcs will always be available since the last loop round
            # has saved the inputs.
            log_produced_calc = log.get_calc(comp)
            assert log_produced_calc == calc

            # Results should obviously not be readable until we save them.
            with pytest.raises(boyleworkflow.NotFoundException):
                log.get_result(calc, comp.loc)


        log.save_run(calc, results, t, t)

        for result in results:
            logged_result = log.get_result(calc, result.loc)
            assert logged_result == result
