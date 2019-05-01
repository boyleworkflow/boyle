from typing import Iterable, Sequence, Mapping, Callable, List, Set

from typing_extensions import Protocol

import itertools
import uuid
import datetime

import attr


class Environment(Protocol):
    def destroy(self):
        pass

EvironmentCreator = Callable[[], Environment]

class Loc(Protocol):
    pass


class Digest(Protocol):
    pass


class Task(Protocol):
    out_locs: Iterable[Loc]

    def run(self, env: Environment):
        ...


@attr.s(auto_attribs=True)
class Calc:
    task: Task
    inputs: Mapping[Loc, Digest]


@attr.s(auto_attribs=True)
class Comp:
    task: Task
    inputs: Mapping[Loc, Comp]
    out_loc: Loc


@attr.s(auto_attribs=True)
class Run:
    run_id: str
    calc: Calc
    results: Mapping[Loc, Digest]
    start_time: datetime.datetime
    end_time: datetime.datetime


class NotFoundException(Exception):
    pass


class Log(Protocol):
    def get_result(self, calc: Calc, out_loc: Loc) -> Digest:
        ...

    def get_calc(self, comp: Comp) -> Calc:
        ...

    def save_response(
        self, comp: Comp, digest: Digest, time: datetime.datetime
    ):
        ...

    def save_run(self, run: Run):
        ...


class Storage(Protocol):
    def can_restore(self, digest: Digest) -> bool:
        ...

    def restore(self, env: Environment, loc: Loc, digest: Digest):
        ...

    def store(self, env: Environment, loc: Loc) -> Digest:
        ...


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


def _run_calc(calc: Calc, log: Log, storage: Storage, create_environment: EvironmentCreator):
    env = create_environment()

    try:
        for loc, digest in calc.inputs.items():
            storage.restore(env, loc, digest)

        start_time = datetime.datetime.utcnow()
        calc.task.run(env)
        end_time = datetime.datetime.utcnow()

        results = {}
        for loc in calc.task.out_locs:
            digest = storage.store(env, loc)
            results[loc] = digest

    finally:
        env.destroy()

    run = Run(
        run_id=str(uuid.uuid4()),
        calc=calc,
        results=results,
        start_time=start_time,
        end_time=end_time,
    )

    log.save_run(run)


def _ensure_available(requested, log, storage, create_environment):
    while True:
        comps_to_run = _get_ready_and_needed(requested, log, storage)
        if not comps_to_run:
            break

        comps_by_calc = defaultdict(set)
        for comp in comps_to_run:
            calc = log.get_calc(comp)
            comps_by_calc[calc].add(comp)

        for calc, comps in comps_by_calc.items():
            _run_calc(calc, log, storage)


def request(
    requested: Iterable[Comp],
    log: Log,
    storage: Storage,
    create_environment: Callable[[], Environment],
):
    time = datetime.datetime.utcnow()
    _ensure_available(requested, log, storage, create_environment)

    results = {}
    for comp in requested:
        calc = log.get_calc(comp)
        digest = log.get_result(calc, comp.out_loc)
        log.save_response(comp, digest, time)
        results[comp] = digest

    return comp
