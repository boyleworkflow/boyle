# -*- coding: utf-8 -*-

import os
import hashlib
import shutil
import subprocess
import csv
import json
import networkx as nx
from collections import namedtuple
from collections import defaultdict
import logging
from uuid import uuid4
import time
import shelve

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)

def hexdigest(str_or_unicode):
    return hashlib.sha1(str_or_unicode.encode('utf-8')).hexdigest()

def digest_file(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()


class ConflictException(Exception):
    """docstring for ConflictException"""
    def __init__(self, calc_id, path):
        super(ConflictException, self).__init__()
        self.calc_id = calc_id
        self.path = path
       

class Log(object):
    """docstring for Log"""
    def __init__(self, path):
        super(Log, self).__init__()
        # self.path = os.path.abspath(path)
        # if not os.path.isdir(self.path):
        #     raise ValueError("could not find a log at '{}'".format(self.path))
        self._shelf = shelve.open(path, writeback=True)
        if not 'runs' in self._shelf:
            self._shelf['runs'] = {}

    def find_output(self, calculation, path):
        """Try to find the digest of a calculation output.

        Args:
            calculation (Calculation): The calculation in question.
            path (str): The relative path of the output.

        Returns:
            None: If the output cannot be found.
            str: A file digest, if exactly one is found.

        Raises:
            ConflictException: If there are more than one candidate values.
        """
        logger.debug('Searching for output {}: {}'.format(calculation, path))
        runs = (r for r in self._shelf['runs'].values() if r.calculation == calculation)
        results = defaultdict(list)
        for r in runs:
            digest = r.output_fsos[path]
            results[digest].append(r)
        if len(results) == 0:
            logger.debug('No output for {}, {}'.format(calculation, path))
            return None
        elif len(results) == 1:
            logger.debug('Output for {}, {}: {}'.format(calculation, path, results.keys()[0]))
            return results.keys()[0]
        else:
            raise ConflictException(calculation.id, path)

    def find_supporters(self, calculation, path, digest):
        """Find all runs that support a certain result."""
        candidates = (r for r in self._shelf['runs'].values() if r.calculation == calculation)
        def is_supporter(run):
            return run.output_fsos[path] == digest

        return list(run for run in candidates if run.output_fsos[path] == digest)
       
    def save_run(self, run):
        """Save a Run."""
        self._shelf['runs'][run.id] = run
        

    def get_conflict(self, calc_id, path):
        """Get info about a conflict over an output of a calculation.

        Args:
            calc_id (str): The id of the calculation.
            path (str): The relative path of the output.

        Returns:
            A list of lists, containing all the Run objects that are
            involved in the conflict. All the runs in each list are
            in agreement with each other.
        """
        pass

    def get_provenance(self, digest):
        return list(r for r in self._shelf['runs'].values() if digest in r.output_fsos.values())


class Storage(object):
    """docstring for Storage"""
    def __init__(self, path):
        super(Storage, self).__init__()
        self.path = os.path.abspath(path)

    def has_file(self, digest):
        """Check if a file exists in the storage.

        Args:
            digest (str): A file digest.

        Returns:
            bool: A value indicating whether the file is available.

        """
        return os.path.exists(os.path.join(self.path, digest))


    def copy_to(self, digest, path):
        """Copy a file from the storage.

        Args:
            digest (str): A file digest.
            path (str): A path to put the file at.

        Raises:
            KeyError: If the file does not exist in the storage.
        """
        src_path = os.path.join(self.path, digest)
        if not os.path.exists(src_path):
            raise KeyError
        shutil.copy2(src_path, path)



    def save(self, path):
        """Save a file in the storage.

        Note:
            The file is moved to the storage, not copied.

        Args:
            path (str): A path where the file is located.

        Returns:
            str: The digest of the file.
        """
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        digest = digest_file(path)
        dst_path = os.path.join(self.path, digest)
        shutil.move(path, dst_path)

        return digest
        


class Run(object):
    """docstring for Run"""
    def __init__(self, calculation, output_fsos, info, supporters):
        super(Run, self).__init__()
        self.id = uuid4()
        self.calculation = calculation
        self.output_fsos = output_fsos
        self.info = info
        self.supporters = supporters

        # Should contain (or be able to report)
        # (1) Calculation the Run carried out
        # (2) FSOs used as inputs
        # (3) For each used input FSO, a list of Runs that have produced that input FSO
        # (4) FSOs produced by this Run

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return type(self) == type(other) and self.id == other.id

    def __repr__(self):
        return '<Run {}>'.format(self.id)
        


class Calculation(object):
    """docstring for Calculation"""
    def __init__(self, task, input_fsos):
        super(Calculation, self).__init__()
        self.task = task
        self.input_fsos = input_fsos.copy()
        id_dict = dict(task=task.id, inputs=input_fsos)
        self.id = hexdigest(unique_json(id_dict))

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return type(self) == type(other) and self.id == other.id

    def __repr__(self):
        return '<Calculation {}>'.format(self.id)


FileSystemObject = namedtuple('FileSystemObject', ['path', 'digest'])

class Graph(object):
    """docstring for Graph"""
    def __init__(self, log, storage):
        super(Graph, self).__init__()
        self.log = log
        self.storage = storage
        self._graph = nx.DiGraph()
        self._tasks = set()
        self._outputs = set()
        self._fsos = dict()
        self._calculations = dict()

    def add_task(self, task):
        # TODO: Check if task is already in graph. If so, silently skip?

        if any(output in self._graph for output in task.outputs):
            raise ValueError('duplicate outputs not allowed')

        for inp in task.inputs:
            self._graph.add_edge(inp, task)

        for output in task.outputs:
            self._graph.add_edge(task, output)

        self._outputs.update(task.outputs)
        self._tasks.add(task)

    def _get_calc(self, task):
        return Calculation(task, {path: self._fsos[path] for path in task.inputs})

    def ensure_exists(self, *requested_outputs):       
        ancestors = set.union(*(nx.ancestors(self._graph, output) for output in requested_outputs))
        ancestor_graph = nx.subgraph(self._graph, ancestors)
        task_graph = nx.project(ancestor_graph, self._tasks)

        for task in nx.topological_sort(task_graph):
            calculation = self._get_calc(task)
            if not all(self.log.find_output(calculation, p) for p in task.outputs):
                self._run(calculation)
            else:
                logger.info('{} has been run previously'.format(calculation))
           
            for path in task.outputs:
                fso = self.log.find_output(calculation, path)
                self._fsos[path] = fso
                if path in requested_outputs and not self.storage.has_file(fso):
                    self._run(calculation)


    def _run(self, calculation):

        for path, digest in calculation.input_fsos.items():
            if not self.storage.has_file(digest):
                parent_task, = self._graph.predecessors(path)
                parent_calc = self._get_calc(parent_task)
                self._run(parent_calc)

        supporters = set()
        for path, digest in calculation.input_fsos.items():
            parent_task, = self._graph.predecessors(path)
            parent_calc = self._get_calc(parent_task)
            supporters.update(self.log.find_supporters(parent_calc, path, digest))

        logger.info('Running {}'.format(calculation))

        # create temp dir
        # run task.prepare(...)
        # copy input files
        # invoke task
        # archive input files
        # save run info in log

        workdir = os.path.join('/tmp/.gpc/', calculation.id)
        if os.path.exists(workdir):
            logger.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)
        logger.debug('creating {}'.format(workdir))
        os.makedirs(workdir)

        task = calculation.task
        for path, digest in calculation.input_fsos.items():
            self.storage.copy_to(digest, os.path.join(workdir, path))
       
        logger.debug('running in {}'.format(workdir))
        task.run(workdir)

        output_fsos = {}
        for path in task.outputs:
            dst_path = os.path.join(workdir, path)
            digest = self.storage.save(dst_path)
            output_fsos[path] = digest

        info = 'I computed this at t={}.'.format(time.time())

        run = Run(calculation, output_fsos, info, supporters)
        self.log.save_run(run)

        if os.path.exists(workdir):
            logger.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)


    def make(self, output):
        self.ensure_exists(output)
        self.storage.copy_to(self._fsos[output], output)


class ShellTask(object):
    """docstring for ShellTask"""
    def __init__(self, command, inputs, outputs):
        super(ShellTask, self).__init__()
        self.command = command
        self.inputs = inputs
        self.outputs = outputs
        self.id = command

    def run(self, workdir):
        original_wd = os.getcwd()
        os.chdir(workdir)
        try:
            subprocess.call(self.command, shell=True)
        except Exception, e:
            raise e
        finally:
            os.chdir(original_wd)

def print_run(run, level=1):
    spacing = '   ' * level
    def pr(val):
        print('%s%s' % (spacing, val))
    pr(run)
    pr(run.calculation)
    pr(run.info)
    for path, digest in run.calculation.input_fsos.items():
        pr(' Input %s (%s)' % (path, digest))
        for i, sr in enumerate(r for r in run.supporters if path in r.output_fsos):
                pr('  Supporter #%i' % (i+1))
                print_run(sr, level=level+1)
    pr('')


def main():
    t1 = ShellTask('echo hello > a', [], ['a'])
    t2 = ShellTask('echo world > b', [], ['b'])
    t3 = ShellTask('cat a b > c', ['a', 'b'], ['c'])

    log = Log('log')
    storage = Storage('storage')
    g = Graph(log, storage)
    g.add_task(t1)
    g.add_task(t2)
    g.add_task(t3)
    g.make('c')

    
    responsible_runs = log.get_provenance(digest_file('c'))
    print('The file was produced by %i run(s):' % len(responsible_runs))
    for r in responsible_runs:
        print_run(r)


if __name__ == '__main__':
    main()