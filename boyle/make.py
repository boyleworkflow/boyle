from typing import Iterable, Sequence, Mapping, Callable, List, Set
import datetime
import itertools
from collections import defaultdict

from boyle.core import NotFoundException
from boyle.protocols import Log, Storage, Loc, Digest, Op, Calc, Comp


def _determine_sets(comps: Iterable[Comp], log: Log, storage: Storage):
    sets: Mapping[str, set] = {
        name: set()
        for name in (
            "Abstract",  # input digests unknown
            "Concrete",  # input digests known
            "Known",  # output digest known
            "Unknown",  # output digest unknown
            "Restorable",  # output data can be restored
            "Runnable",  # input data can be restored
        )
    }

    for comp in comps:
        input_comps = set(comp.inputs.values())
        if input_comps <= sets["Known"]:
            sets["Concrete"].add(comp)
        else:
            sets["Abstract"].add(comp)
            continue

        if input_comps <= sets["Restorable"]:
            sets["Runnable"].add(comp)

        calc = log.get_calc(comp)
        try:
            digest = log.get_result(calc, comp.out_loc)
            sets["Known"].add(comp)
            if storage.can_restore(digest):
                sets["Restorable"].add(comp)
        except NotFoundException as e:
            sets["Unknown"].add(comp)

    return sets


def _get_parents(comps: Iterable[Comp]) -> Iterable[Comp]:
    return list(itertools.chain(*(comp.inputs.values() for comp in comps)))


def _get_upstream_sorted(requested: Iterable[Comp]) -> Sequence[Comp]:
    chunks: List[Iterable[Comp]] = []
    new: Iterable[Comp] = list(requested)
    while new:
        chunks.insert(0, new)
        new = _get_parents(new)
    return list(itertools.chain(*chunks))


def _get_ready_and_needed(requested, log, storage) -> Iterable[Comp]:
    requested = set(requested)
    assert len(requested) > 0

    comps = _get_upstream_sorted(requested)
    sets = _determine_sets(comps, log, storage)

    if requested <= sets["Restorable"]:
        return set()

    candidates: Set[Comp] = set()

    # First set of candidates is the union of
    #  (1) requested outputs that are not restorable, and
    #  (2) all unknown outputs upstream of the requested outputs
    additional = (requested - sets["Restorable"]) | sets["Unknown"]

    while additional:
        candidates.update(additional)

        # Furthermore we need to run parents to the previous candidates,
        # if those parents are not restorable.
        additional = set(_get_parents(additional)) - sets["Restorable"]

    final = candidates.intersection(sets["Runnable"])
    assert len(final) > 0, len(sets["Runnable"])

    return final


def _run_calc(calc: Calc, out_locs: Iterable[Loc], log: Log, storage: Storage):

    start_time = datetime.datetime.utcnow()
    results = calc.op.run(calc.inputs, storage)
    end_time = datetime.datetime.utcnow()

    for digest in results.values():
        assert storage.can_restore(digest), digest

    log.save_run(
        calc=calc, results=results, start_time=start_time, end_time=end_time
    )


def _ensure_available(requested, log, storage):
    while True:
        comps_to_run = _get_ready_and_needed(requested, log, storage)
        if not comps_to_run:
            break

        comps_by_calc = defaultdict(set)
        for comp in comps_to_run:
            calc = log.get_calc(comp)
            comps_by_calc[calc].add(comp)

        for calc, comps in comps_by_calc.items():
            out_locs = set(comp.out_loc for comp in comps)
            _run_calc(calc, out_locs, log, storage)


def make(requested: Iterable[Comp], log: Log, storage: Storage):
    time = datetime.datetime.utcnow()
    _ensure_available(requested, log, storage)

    results = {}
    for comp in requested:
        calc = log.get_calc(comp)
        digest = log.get_result(calc, comp.out_loc)
        log.save_response(comp, digest, time)
        results[comp] = digest

    return results
