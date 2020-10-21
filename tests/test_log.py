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


@pytest.fixture
def log(request):

    temp_dir = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(temp_dir)

    request.addfinalizer(fin)

    log_path = os.path.join(temp_dir, "log.db")
    log = boyleworkflow.Log(log_path)

    return log
