#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `boyleworkflow` package."""

import tempfile
import shutil
import os

import pytest

from boyleworkflow.core import Comp, Op
from boyleworkflow.loc import Loc, SpecialFilePath


disallowed_locs = ["", "..", "a/../b", "/", "/home/user", '../x']
allowed_locs = {
    "a..b": "a..b",
    "a/x..": "a/x..",
    "a/.": "a",
    "a/./b/c/.": "a/b/c",
    "a/./b": "a/b",
    ".": ".",
    }


def test_allowed_loc():
    op = Op()

    for loc in disallowed_locs:
        with pytest.raises(ValueError):
            Comp(op, (), loc)

    for inp, expected_out in allowed_locs.items():
        comp = Comp(op, (), inp)
        assert comp.loc == expected_out

