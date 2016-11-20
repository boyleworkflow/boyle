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

def unique_str_helper(func):
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
    @unique_str_helper
    def unique_str(self):
        return {k: v.unique_str for k, v in attr.asdict(self).items}

@attr.s
class Calculation:
    procedure = attr.ib()
    inputs = attr.ib()

    @property
    @unique_str_helper
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


@attr.s
class File:
    relpath = attr.ib()

    @property
    @unique_str_helper
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
    @unique_str_helper
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
