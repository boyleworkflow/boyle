import collections
from itertools import chain
import tempfile
import os
import shutil
import subprocess
import logging
import hashlib

logger = logging.getLogger(__name__)

def _walk_backwards(definitions):
    layer = definitions[:]
    next_layer = set()
    visited = set()
    while len(layer) > 0:
        for d in layer:
            if d in visited:
                raise ValueError('the definitions have circular dependencies')
            next_layer.update(d.inputs)
            visited.add(d)
            yield d
        layer = next_layer

def get_sorted_upstream(definitions):
    return list(_walk_backwards(definitions))[-1::-1]

def deliver(requested_defs, delivery_dir):
    storages = {}
    digests = {}

    def setup_storage(definition, graph_dir):
        assert definition not in storages
        store_dir = tempfile.mkdtemp(prefix='store_', dir=graph_dir)
        storages[d] = d.resource_handler.create_temp_storage(store_dir)

    def restore(definition, destination_dir):
        d = definition
        assert d in digests
        assert d in storages
        d.resource_handler.restore(digests[d], storages[d], destination_dir)

    def save(definition, work_dir):
        d = definition
        assert d in storages
        assert d not in digests
        digests[d] = d.resource_handler.save(work_dir, storages[d])

    if isinstance(requested_defs, ResourceDefinition):
        requested_defs = (requested_defs,)
    delivery_dir = os.path.abspath(delivery_dir)
    with tempfile.TemporaryDirectory() as graph_dir:
        graph_dir = os.path.abspath(graph_dir)
        for d in get_sorted_upstream(requested_defs):
            setup_storage(d, graph_dir)
            work_dir = tempfile.mkdtemp(prefix='work_', dir=graph_dir)

            for inp in d.inputs:
                restore(inp, work_dir)

            for item in d.recipe:
                item.run(work_dir)

            save(d, work_dir)

            if d in requested_defs:
                restore(d, delivery_dir)

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
        raise ValueError('all the recipe items must be callable')

    # TODO: Which sorts of inputs could out be, really? It seems to make sense
    # that it can be compositions of lists/tuples and dicts, where all leaf
    # nodes are ResourceHandlers.
    if not isinstance(out, collections.Sequence):
        out = (out,)

    defs = tuple(ResourceDefinition(inp, out_item, do) for out_item in out)

    if len(defs) == 1:
        return defs[0]
    else:
        return defs


class ResourceDefinition:

    inputs = None
    resource_handler = None
    recipe = None

    def __init__(self, inputs, resource_handler, recipe):
        self.inputs = inputs
        self.resource_handler = resource_handler
        self.recipe = recipe



def digest_file(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()


class File:

    def __init__(self, relpath):
        self._relpath = relpath

    def _make_storage_path(self, storage_dir, digest):
        return os.path.join(storage_dir, digest)

    def create_temp_storage(self, store_dir):
        return store_dir

    def restore(self, digest, storage_dir, work_dir):
        storage_path = self._make_storage_path(storage_dir, digest)
        restore_path = os.path.join(work_dir, self._relpath)
        logger.debug(
            'restoring from {} to {}'.format(storage_path, restore_path))
        shutil.copy(storage_path, restore_path)

    def save(self, work_dir, storage_dir):
        file_path = os.path.join(work_dir, self._relpath)
        digest = digest_file(file_path)
        storage_path = self._make_storage_path(storage_dir, digest)
        logger.debug('saving from {} to {}'.format(file_path, storage_path))
        shutil.copy(file_path, storage_path)
        return digest


class Shell:

    def __init__(self, cmd):
        self._cmd = cmd

    def run(self, work_dir):
        # TODO: Think about safety here.
        # Should the shell=True variant be called UnsafeShell?
        # Perhaps at least chroot the subprocess by default?
        # Boyle cannot and should not prevent arbitrary code execution, but
        # chroot and/or similar measures could at least prevent some mistakes.
        logger.debug("running cmd '{}' in '{}'".format(self._cmd, work_dir))
        proc = subprocess.Popen(self._cmd, shell=True, cwd=work_dir)
        proc.wait()
