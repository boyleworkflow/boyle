#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `boyleworkflow` package."""

import tempfile
import shutil
import os

import pytest

import boyleworkflow


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


def restore_and_read(digest, storage, mode="r"):
    with tempfile.TemporaryDirectory() as td:
        path = f"{td}/tempfile"
        storage.restore(digest, path)
        with open(path, mode) as f:
            return f.read()


def make_and_check_expected_contents(expected_file_contents, log, storage):

    assert len(expected_file_contents) > 0

    results = boyleworkflow.make(list(expected_file_contents), log, storage)

    assert set(results) == set(expected_file_contents)

    for comp, digest in results.items():
        result = restore_and_read(digest, storage)
        assert result == expected_file_contents[comp]
