import os
import tempfile
import subprocess
from pathlib import Path
import datetime

import attr

from boyleworkflow.core import Op, Resources, Loc, Digest, Storage, JsonDict


def run_op(op: Op, inputs: Resources, storage: Storage) -> Resources:
    func = _FUNCTIONS[op.op_type]
    return func(inputs, storage, options)


def write_bytes(
    inputs: Resources, storage: Storage, options: JsonDict
) -> Resources:
    data = options["data"]
    assert isinstance(data, bytes), data
    loc = Loc(options["loc"])
    digest = storage.write_bytes(data)
    return {loc: digest}


_FUNCTIONS = dict(write_bytes=write_bytes)
