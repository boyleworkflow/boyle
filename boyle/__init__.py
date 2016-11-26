import collections
import os
import shutil
import subprocess
import logging
import hashlib

import attr

logger = logging.getLogger(__name__)

def digest_str(s):
    hashlib.sha1(s.encode('utf-8')).hexdigest()

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)

def _apply_leaves(func, obj):
    if isinstance(obj, dict):
        return {k: _apply_leaves(v, func) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_apply_leaves(v, func) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(_apply_leaves(v, func) for v in obj)
    else:
        return func(obj)

def _unique_str_helper(func):
    @functools.wraps(func)
    def wrapper(obj):
        return unique_json([obj.__qualname__, func(obj)])

    return wrapper

class Storage:
    def __init__(storage_dir):
        self._storage_dir = storage_dir

    def get_dir(obj):
        """
        Get a directory devoted to an object.
        Args:
            obj: Any object which has a unique_str attribute.

        Returns:
            Path to the directory reserved for the object in this storage,
            or actually reserved for the unique_str value.
        """
        path = os.path.join(self._storage_dir, digest_str(obj.unique_str))
        os.makedirs(path, exist_ok=True)
        return path


class Log:
    def __init__(log_dir):
        self._log_dir = log_dir
        self._results = {}

    def get_result(calculation, instrument, tmax):
        return self._results[(calculation, instrument)]

@attr.s
class Definition:
    inputs = attr.ib()
    procedure = attr.ib()
    instrument = attr.ib()

@attr.s
class Resource:
    instrument = attr.ib()
    digest = attr.ib()

    @property
    @_unique_str_helper
    def unique_str(self):
        return {k: v.unique_str for k, v in attr.asdict(self).items}

    def save(self, context, storage):
        storage_dir = storage.get_dir(self)
        self.instrument.save(context, storage_dir)

    def restore(self, storage, context):
        storage_dir = storage.get_dir(self)
        self.instrument.restore(storage_dir, context)

@attr.s
class Calculation:
    procedure = attr.ib()
    inputs = attr.ib()

    @property
    @_unique_str_helper
    def unique_str(self):
        return _apply_leaves(lambda x: x.unique_str, attr.asdict(self))

def define(inp=None, out=None, do=None):
    # TODO: Plenty more input validation, since this is the most central
    # part of the API from the end user's perspective
    if out is None:
        raise ValueError('the definition must define something')
    if hasattr(do, 'run'):
        do = (do,)
    inp = () if inp is None else tuple(inp)
    do = () if do is None else tuple(do)
    if not all(callable(item.run) for item in do):
        raise ValueError('all the operations must be callable')

    # TODO: Which sorts of inputs could out be, really? It seems to make sense
    # that it can be compositions of lists/tuples and dicts, where all leaf
    # nodes are ResourceHandlers.
    if not isinstance(out, collections.Sequence):
        out = (out,)

    defs = tuple(
        Definition(inputs=inp, procedure=do, instrument=out_item)
        for out_item in out)

    if len(defs) == 1:
        return defs[0]
    else:
        return defs


def _digest_file(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()


def _known_unknown(requested, is_known):
    root_nodes = set()
    children = collections.defaultdict(set)

    visited = set()
    candidates = set(requested_defs)
    while candidates:
        d = candidates.pop()
        if d in visited:
            continue

        if d.inputs:
            candidates.update(d.inputs)
            for parent in d.inputs:
                children[parent].add(d)
        else:
            root_nodes.add(d)

    known = set()
    unknown = set()

    nodes = root_nodes
    while nodes:
        new_known = {d for d in nodes if is_known(d)}
        known.update(new_known)
        unknown.update(nodes - new_known)
        nodes = set.union(*children(d) for d in new_known)

    return known, unknown


def _resolve(requested_defs, log, storage):
    """
    Tries to resolve the Resources defined by Definitions defs.

    Args:
        requested_defs: An iterable of requested Definitions.
        log: A Log to read from and write in.
        storage: A Storage for resources.

    Returns:
        A dictionary of {definition: resource} pairs.

    Raises:
        RunRequired, if additional calculations must be run before
        the defs can be resolved.
    """

    # Mathematically this is seen as a function mapping
    # (Time, User, Definition) -> Resource | Requirement[]
    # So the first thing to note is
    # time = now
    # user = ctx.user

    def get_calculation(definition):
        return Calculation(
            definition.procedure,
            [get_result(inp) for inp in definition.inputs])

    def get_result(definition):
        log.get_result(
            calculation=get_calculation(definition),
            instrument=definition.instrument,
            tmax=time,
            )

    def is_known(definition):
        try:
            get_result(definition)
            return True
        except NoResult:
            return False

    def is_restorable(definition):
        resource = get_result(definition)
        return definition.instrument.can_restore(storage)

    def find_more_needed(initial):
        new = initial
        needed = set()
        while new:
            needed.update(new)
            children = candidates[-1]
            all_parents = set.union(*set(c.inputs) for c in children)
            new = {p for p in all_parents if not is_restorable(p)}
        return needed

    requested_defs = set(requested_defs)
    known, unknown = _known_unknown(requested_defs, is_known)
    definitely_needed = (
        {d for d in (requested_defs & known) if not is_restorable(definition)}
        | unknown
        )
    run_needed = find_more_needed(definitely_needed)

    if run_needed:
        needed_and_possible = filter(is_runnable, run_needed)
        calcs_instruments = collections.defaultdict(set)
        for d in needed_and_possible:
            calcs_instruments[get_calculation(d)].add(d.instrument)
        raise RunRequired(calcs_instruments)

    return {d: get_result(d) for d in requested_defs}


def ensure_restorable(requested_defs, log, storage):
    run_needed = {}
    while True:
        for calc, instruments in run_needed.items():
            _run(calc, instruments, log, storage)
        try:
            return _resolve(requested_defs, log, storage)
        except RunRequired as e:
            run_needed = e.calculations


@attr.s
class File:
    relpath = attr.ib()

    @property
    @_unique_str_helper
    def unique_str(self):
        return self.relpath

    def _storage_path(self, storage_dir):
        return os.path.join(storage_dir, self.relpath)

    def _context_path(self, context):
        os.path.join(context.work_dir, self.relpath)

    def digest(self, context):
        file_path = self._context_path(context)
        digest = _digest_file(file_path)
        return digest

    def restore(self, storage_dir, context):
        storage_path = self._storage_path(storage_dir)
        restore_path = self._context_path(context)
        logger.debug(
            'restoring from {} to {}'.format(storage_path, restore_path))
        shutil.copy(storage_path, restore_path)

    def save(self, context, storage_dir):
        file_path = self._context_path(context)
        storage_path = self._storage_path(storage_dir)
        logger.debug('saving from {} to {}'.format(file_path, storage_path))
        shutil.copy(file_path, storage_path)


@attr.s
class Shell:
    cmd = attr.ib()

    @property
    @_unique_str_helper
    def unique_str(self):
        return self.cmd

    def run(self, work_dir):
        # TODO: Think about safety here.
        # Should the shell=True variant be called UnsafeShell?
        # Perhaps at least chroot the subprocess by default?
        # Boyle cannot and should not prevent arbitrary code execution, but
        # chroot and/or similar measures could at least prevent some mistakes.
        logger.debug("running cmd '{}' in '{}'".format(self.cmd, work_dir))
        proc = subprocess.Popen(self.cmd, shell=True, cwd=work_dir)
        proc.wait()
