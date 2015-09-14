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

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)

def hexdigest(str_or_unicode):
    return hashlib.sha1(str_or_unicode.encode('utf-8')).hexdigest()

def digest_file(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()


class Repository(object):
    """docstring for Repository"""
    def __init__(self, path):
        super(Repository, self).__init__()
        gpc_dir = os.path.abspath(os.path.join(path, '.gpc'))
        if not os.path.isdir(gpc_dir):
            raise ValueError("could not find a repo at '{}'".format(gpc_dir))
        self._path = os.path.abspath(path)
        self._runs_path = os.path.join(gpc_dir, 'runs')
        self._archive_path = os.path.join(gpc_dir, 'archive')

    def find_results(self, calc):
        with open(self._runs_path, 'a+') as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == calc.id:
                    return json.loads(row[1])
        return None

    def save_results(self, calc, results):
        try:
            previous = self.find_results(calc)
            if previous is not None and previous != results:
                raise ValueError('{} already exists with other result'.format(calc))
        except KeyError:
            pass

        with open(self._runs_path, 'a') as f:
            writer = csv.writer(f)
            writer.writerow([calc.id, unique_json(results)])

    def _data_path(self, digest):
        return os.path.join(self._archive_path, digest)

    def find_data(self, digest):
        return os.path.exists(self._data_path(digest))

    def copy_from_archive(self, digest, dst_path):
        src_path = self._data_path(digest)
        dst_path = os.path.join(self._path, dst_path)
        shutil.copy2(src_path, dst_path)

    def move_to_archive(self, src_path):
        if not os.path.exists(self._archive_path):
            os.makedirs(self._archive_path)
        digest = digest_file(src_path)
        shutil.move(src_path, self._data_path(digest))
        return digest


class Calculation(object):
    """docstring for Calculation"""
    def __init__(self, task, indata):
        super(Calculation, self).__init__()
        self.task = task
        self.indata = indata
        calc_desc = dict(task=task.task_id(), **{item.path: item.digest for item in indata})
        self.id = hexdigest(unique_json(calc_desc))

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return type(self) == type(other) and self.id == other.id

    def __repr__(self):
        return '<Calculation {}>'.format(self.id)


DataItem = namedtuple('DataItem', ['path', 'digest'])

class Graph(object):
    """docstring for Graph"""
    def __init__(self, repo):
        super(Graph, self).__init__()
        self._repo = repo
        self._graph = nx.DiGraph()
        self._tasks = set()
        self._outputs = set()
        self._cache = defaultdict(dict)

    def add_task(self, task):
        if any(output in self._graph for output in task.outputs):
            raise ValueError('duplicate outputs not allowed')

        for inp in task.inputs:
            self._graph.add_edge(inp, task)

        for output in task.outputs:
            self._graph.add_edge(task, output)

        self._outputs.update(task.outputs)
        self._tasks.add(task)

    def ensure_exists(self, *requested_outputs):
        cache = self._cache
        
        ancestors = set.union(*(nx.ancestors(self._graph, output) for output in requested_outputs))
        ancestor_graph = nx.subgraph(self._graph, ancestors)
        task_graph = nx.project(ancestor_graph, self._tasks)

        for task in nx.topological_sort(task_graph):
            inputs = (DataItem(path=path, digest=cache[path]['digest']) for path in task.inputs)
            calculation = Calculation(task, inputs)
            for path in task.outputs:
                cache[path]['calculation'] = calculation
            if not self._repo.find_results(calculation):
                self._run(calculation)
            else:
                log.info('{} has been run previously'.format(calculation))
            
            results = self._repo.find_results(calculation)

            for path in task.outputs:
                cache[path]['digest'] = results[path]
                if path in requested_outputs and not self._repo.find_data(cache[path]['digest']):
                    self._run(calculation)

    def _run(self, calculation):
        cache = self._cache
        for path in calculation.task.inputs:
            if not self._repo.find_data(cache[path]['digest']):
                self._run(cache[path]['calculation'])

        log.info('Running {}'.format(calculation))

        # create temp dir
        # run task.prepare(...)
        # copy input files
        # invoke task
        # if previous run of same calculation in repo, check_reproduced(result)
        # archive input files

        workdir = os.path.join('/tmp/.gpc/', calculation.id)
        if os.path.exists(workdir):
            log.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)
        log.debug('creating {}'.format(workdir))
        os.makedirs(workdir)

        task = calculation.task
        for path in task.inputs:
            self._repo.copy_from_archive(cache[path]['digest'], os.path.join(workdir, path))
        
        log.debug('running in {}'.format(workdir))
        task.run(workdir)

        results = {}
        for output in task.outputs:
            dst_path = os.path.join(workdir, output)
            digest = self._repo.move_to_archive(dst_path)
            results[output] = digest
        self._repo.save_results(calculation, results)

        if os.path.exists(workdir):
            log.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)


    def make(self, output):
        self.ensure_exists(output)
        self._repo.copy_from_archive(self._cache[output]['digest'], output)


class ShellTask(object):
    """docstring for ShellTask"""
    def __init__(self, command, inputs, outputs):
        super(ShellTask, self).__init__()
        self.command = command
        self.inputs = inputs
        self.outputs = outputs

    def task_id(self):
        return self.command

    def run(self, workdir):
        original_wd = os.getcwd()
        os.chdir(workdir)
        try:
            subprocess.call(self.command, shell=True)
        except Exception, e:
            raise e
        finally:
            os.chdir(original_wd)


def run_python_task(gpc_dir, work_dir, func_name, args):
    """
    import gpc
    chdir({work_dir})
    import {module}
    values = {name: gpc.load_value()
    result = {module}.{func_name}()
    gpc.save_value('{calculation_id}' '{name}', result)
    ...
    """


def main():
    t1 = ShellTask('echo hello > a', [], ['a'])
    t2 = ShellTask('echo world > b', [], ['b'])
    t3 = ShellTask('cat a b > c', ['a', 'b'], ['c'])

    r = Repository('')
    g = Graph(r)
    g.add_task(t1)
    g.add_task(t2)
    g.add_task(t3)
    g.make('c')


if __name__ == '__main__':
    main()