#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `boyleworkflow` package."""

import tempfile
import shutil
import os

import pytest

import boyleworkflow
from boyleworkflow.loc import SpecialFilePath
from boyleworkflow.api import shell, rename, Task


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


def test_simple_shell(log, storage):
    a = shell("echo hello > a").out("a")
    b = shell("echo world > b").out("b")
    c = shell("cat a b > c && echo test", inputs=(a, b)).out("c")

    expected_file_contents = {a: "hello\n", b: "world\n", c: "hello\nworld\n"}

    make_and_check_expected_contents(expected_file_contents, log, storage)


def test_stdin(log, storage):
    a = shell("echo 'testing stdin' > a").out("a")
    b = shell("cat > b", stdin=a).out("b")

    make_and_check_expected_contents({b: "testing stdin\n"}, log, storage)


def test_stdout_output(log, storage):
    task = shell("echo something > a && echo 'testing stdout'")

    make_and_check_expected_contents(
        {task.out("a"): "something\n", task.stdout: "testing stdout\n"},
        log,
        storage,
    )


def test_stdout_input(log, storage):
    a = shell("echo 'like piping'").stdout

    b = shell("cat > b", stdin=a).out("b")

    make_and_check_expected_contents({b: "like piping\n"}, log, storage)


def test_stdout_chain(log, storage):
    a = shell("echo 'like pipin a chain'").stdout

    b = shell("cat", stdin=a).stdout

    c = shell("cat", stdin=b).stdout

    make_and_check_expected_contents({c: "like piping a chain\n"}, log, storage)


def test_stderr(log, storage):
    # redirect stdout to stderr
    # not sure if this is the way to do it...
    task = shell("echo 'testing stderr' 1>&2")
    make_and_check_expected_contents(
        {task.stderr: "testing stderr\n", task.stdout: ""}, log, storage
    )


def test_special_files_access():
    stdout_1 = shell('command').out(SpecialFilePath.STDOUT.value)
    stdout_2 = shell('command').stdout
    assert stdout_1 == stdout_2

    stderr_1 = shell('command').out(SpecialFilePath.STDERR.value)
    stderr_2 = shell('command').stderr
    assert stderr_1 == stderr_2


def test_specal_files_disallowed():
    op = boyleworkflow.core.Op()

    # stdin cannot be an output
    with pytest.raises(ValueError):
        Task(op).out(SpecialFilePath.STDIN.value)

    # stdout can be output but not input
    stdout = shell('command').stdout
    with pytest.raises(ValueError):
        Task(op, [stdout])


    # stderr can be output but not input
    stderr = shell('command').stderr
    with pytest.raises(ValueError):
        Task(op, [stderr])



def test_rename(log, storage):
    # redirect stdout to stderr
    # not sure if this is the way to do it...
    a = shell("echo 'hello world' > a").out('a')
    b = rename(a, 'b')
    c = shell("cp b c", [b]).out('c')

    make_and_check_expected_contents({c: "hello world\n"}, log, storage)
