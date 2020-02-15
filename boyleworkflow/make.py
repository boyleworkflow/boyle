from typing import Set, Sequence, Mapping, Iterable
import contextlib
import os
from pathlib import Path
import datetime

import attr

from boyleworkflow.core import (
    Op,
    Calc,
    Node,
    Defn,
    Digest,
    Index,
    IndexKey,
    Run,
    Log,
    Loc,
    Storage,
    Resources,
    NotFoundException,
    DefnResult,
    SINGLE_KEY,
)
from boyleworkflow.ops import run_op



@attr.s(auto_attribs=True)
class Context:
    log: Log
    storage: Storage
    project_dir: Loc
    temp_dir: Loc
    outdata_dir: Loc


def _run_calc(calc: Calc, ctx: Context):
    start_time = datetime.datetime.utcnow()
    results = run_op(calc.op, calc.inputs, ctx.storage)
    end_time = datetime.datetime.utcnow()
    run = Run(calc, start_time, end_time, results)
    ctx.log.save_run(run)


def _get_upstream_sorted(defns: Iterable[Defn]) -> Sequence[Defn]:
    raise NotImplementedError()


def _read_index(digest: Digest, storage: Storage) -> Index:
    return Index(storage.read_bytes(digest).decode())


def _ensure_defn_restorable(defn: Defn, ctx: Context):
    # The requested Defns are also marked as explicit requests,
    # later when they are placed in the out directory.
    explicit_request = False

    index_digest = ctx.log.get_defn_result(defn.node.index_defn, SINGLE_KEY)
    index = _read_index(index_digest, ctx.storage)
    ctx.log.save_index(index)

    for key in index:
        calc = ctx.log.get_calc(defn.node, key)

        needs_run = True  # assume until proven otherwise

        with contextlib.suppress(NotFoundException):
            digest = ctx.log.get_calc_result(calc, defn.loc)

            if ctx.storage.can_restore(digest):
                needs_run = False

        if needs_run:
            _run_calc(calc, ctx)
            digest = ctx.log.get_calc_result(calc, defn.loc)

        ctx.log.save_defn_result(
            DefnResult(defn, index, key, digest, explicit_request)
        )


def _ensure_all_restorable(requested: Iterable[Defn], ctx: Context):
    upstream_sorted = _get_upstream_sorted(requested)

    for defn in upstream_sorted:
        _ensure_defn_restorable(defn, ctx)


def _key_to_str(key: IndexKey) -> str:
    return str(key)


def _get_out_loc(ctx: Context, name: str, defn: Defn, key: IndexKey) -> Loc:
    return ctx.outdata_dir / name / _key_to_str(key) / defn.loc


def _place_results(request: Mapping[str, Defn], ctx: Context):
    explicit_request = True
    for name, defn in request.items():
        index = ctx.log.get_index(defn.node)
        for key in index:
            digest = ctx.log.get_defn_result(defn, key)

            ctx.log.save_defn_result(
                DefnResult(defn, index, key, digest, explicit_request)
            )

            out_loc = _get_out_loc(ctx, name, defn, key)
            ctx.storage.restore(digest, out_loc)


def make(request: Mapping[str, Defn], ctx: Context):
    requested_defns = set(request.values())
    _ensure_all_restorable(requested_defns, ctx)
    _place_results(request, ctx)
