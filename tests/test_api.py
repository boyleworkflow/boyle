#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `boyleworkflow` package."""

import tempfile
import shutil
import os

import pytest

import boyleworkflow
from boyleworkflow.api import shell


@pytest.fixture
def storage(request):

    temp_dir = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(temp_dir)

    request.addfinalizer(fin)

    storage = boyleworkflow.Storage(temp_dir)

    return storage


@pytest.fixture
def log(request):

    temp_dir = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(temp_dir)

    request.addfinalizer(fin)

    log_path = os.path.join(temp_dir, "log.db")
    log = boyleworkflow.Log(log_path)

    return log


def test_simple_shell(log, storage):
    a = shell("echo hello > a").out("a")
    b = shell("echo world > b").out("b")
    c = shell("cat a b > c && echo test", inputs=(a, b)).out("c")

    expected_file_contents = {a: "hello\n", b: "world\n", c: "hello\nworld\n"}

    results = boyleworkflow.make([a, b, c], log, storage)

    assert set(results) == set(expected_file_contents)

    with tempfile.TemporaryDirectory() as td:

        for comp, digest in results.items():
            path = f"{td}/tempfile"
            storage.restore(digest, path)
            with open(path, "r") as f:
                result = f.read()
            os.remove(path)

            assert result == expected_file_contents[comp]
