import os
import tempfile
import subprocess
from pathlib import Path
import datetime

import attr

from boyleworkflow.core import Op, Loc, Storage, Tree


def run_op(op: Op, input_tree: Tree, storage: Storage) -> Tree:
    raise NotImplementedError()
    # func = _FUNCTIONS[op.op_type]
    # return func(input_tree, storage, options)


# _FUNCTIONS = dict()
