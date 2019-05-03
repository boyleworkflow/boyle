#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `boyleworkflow` package."""

import tempfile
import shutil
import os

import pytest

from click.testing import CliRunner

import boyleworkflow
from boyleworkflow import cli

from boyleworkflow.storage import Storage


@pytest.fixture
def storage(request):

    temp_dir = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(temp_dir)

    request.addfinalizer(fin)

    storage = boyleworkflow.Storage(temp_dir)

    return storage


def test_store_and_restore(storage):

    original_contents = {
        'a': b'content a',
        'b': b'content b',
        'c': b'content a',
    }

    digests = {}

    # Write the files in temp directories that are then removed

    for k, content in original_contents.items():
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, 'my_file')
            with open(p, 'wb') as f:
                f.write(content)

            digests[k] = storage.store(p)


    assert digests['a'] != digests['b']
    assert digests['a'] == digests['c']


    # Restore the files and check that they are the same

    for k, digest in digests.items():
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, 'restored')
            storage.restore(digest, p)

            with open(p, 'rb') as f:
                restored_content = f.read()

            assert restored_content == original_contents[k]
