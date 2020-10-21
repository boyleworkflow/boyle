#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `boyleworkflow` package."""

import tempfile
import shutil
import os
import time

import pytest

from click.testing import CliRunner

import boyleworkflow
from boyleworkflow import cli
from boyleworkflow.util import set_file_permissions
from boyleworkflow.storage import Storage


@pytest.fixture
def storage(request):

    temp_dir = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(temp_dir)

    request.addfinalizer(fin)

    storage = Storage(temp_dir)

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


def test_file_permissions(storage):

    # Restored files should be readable and write protected.
    # No other guarantees on file permissions.
    # In Unix this means file mode 444 or 555 or anything in between,
    # but the Windows write protection is found in the user bit
    # so in practice just check that the user write bit is false.

    original_content = 'abc'

    with tempfile.TemporaryDirectory() as td:
        orig_path = os.path.join(td, 'my_file')

        with open(orig_path, 'w') as f:
            f.write(original_content)

        digest = storage.store(orig_path)

    with tempfile.TemporaryDirectory() as td:
        restored_path = os.path.join(td, 'restored')
        storage.restore(digest, restored_path)

        with pytest.raises(PermissionError):
            open(restored_path, 'a')


def test_modify(storage):

    # Modifications to contents of restored files should not persist.
    # This could happen if we handle hardlinks in a stupid way.
    # However, it is fine if the storage notices changed files and
    # discards them.
    #
    # Therefore there are two acceptable behaviors after changing
    # a restored file:
    #
    # 1. It cannot be restored again.
    # 2. It can be restored again (with the original content).

    original_content = 'abc'

    with tempfile.TemporaryDirectory() as td:
        orig_path = os.path.join(td, 'my_file')

        with open(orig_path, 'w') as f:
            f.write(original_content)

        digest = storage.store(orig_path)


    # Wait a tiny bit before modifying the file;
    # if this is left out it seems the mtime is not updated later,
    # indicating that the file handle has been recycled or something?
    time.sleep(.01)


    with tempfile.TemporaryDirectory() as td:
        restored_path = os.path.join(td, 'restored')
        storage.restore(digest, restored_path)
        set_file_permissions(restored_path, write=True)
        with open(restored_path, 'a') as f:
            f.write('d')


    if storage.can_restore(digest):
        # If the storage can restore,
        # it must have had its own copy of the file

        with tempfile.TemporaryDirectory() as td:
            restored_path = os.path.join(td, 'restored')

            storage.restore(digest, restored_path)

            with open(restored_path, 'r') as f:
                restored_content = f.read()

            assert restored_content == original_content

    else:
        # If the storage can not restore, it probably provided a
        # hardlink that modified the stored file.
        # In this case, we would like the storage to recover
        # if we first call "store" with the same file again.
        with tempfile.TemporaryDirectory() as td:
            orig_path = os.path.join(td, 'my_file')

            with open(orig_path, 'w') as f:
                f.write(original_content)

            storage.store(orig_path)

        assert storage.can_restore(digest)