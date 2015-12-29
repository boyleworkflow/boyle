# -*- coding: utf-8 -*-

import os
import shutil
import subprocess
import json
import networkx as nx
import logging
from uuid import uuid4
import time
from itertools import chain

from gpc.common import hexdigest, digest_file, unique_json
from gpc import fsdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_ID = 1234
USER_NAME = 'aoeu'


class ConflictException(Exception):
    """docstring for ConflictException"""
    def __init__(self, calc_id, path):
        super(ConflictException, self).__init__()
        self.calc_id = calc_id
        self.path = path


class Log(object):
    def __init__(self, path):
        """Create or open a log.

        Args:
            path: Where to create the log. To create a new log,
             pick an empty or nonexistent directory.
        """
        super(Log, self).__init__()
        self.path = os.path.abspath(path)
        if not os.path.exists(path):
            schema_path = os.path.join(
                os.path.dirname(__file__),
                '../database.sql')
            schema = open(schema_path, 'r').read()
            fsdb.Database.create(path, schema)
        self._db = fsdb.Database.load(self.path)

        count = self._db.execute(
            'select count(id) from usr where id = ?',
            (USER_ID,)).fetchone()[0]

        if not count:
            self._db.execute(
                'insert into usr(id, name) values(?, ?)',
                (USER_ID, USER_NAME))

    def find_output(self, calc_id, path):
        """Try to find the digest of a calculation output.

        Args:
            calc_id (str): The id of the calculation in question.
            path (str): The relative path of the output.

        Returns:
            None: If the output cannot be found.
            str: A file digest, if exactly one is found.

        Raises:
            ConflictException: If there are more than one candidate values.
        """
        def is_trusted(run):
            rows = self._db.select('trust', run=run['id'], path=path)
            rows = sorted(rows, key=lambda row: row['time'], reverse=True)
            return rows[0]['correct']

        logger.debug('Searching for output {}: {}'.format(calc_id, path))
        
        trusted_digests = set()
        runs = self._db.select('run', calculation=calc_id)
        logger.debug('Found %i matching run(s):' % len(runs))
        for r in runs:
            logger.debug(' %s' % r['id'])

        for run in filter(is_trusted, runs):
            created = self._db.select('created', run=run['id'], path=path)
            # There should be exactly one file created with that run and path
            assert len(created) == 1
            trusted_digests.add(created[0]['digest'])

        if len(trusted_digests) == 0:
            logger.debug('No trusted output')
            return None
        elif len(trusted_digests) == 1:
            logger.debug('One trusted output: {}'.format(trusted_digests))
            return trusted_digests.pop()
        else:
            logger.debug('More than one trusted output: {}'.format(trusted_digests))
            raise ConflictException(calc_id, path)


    def find_supporters(self, calc_id, path, digest):
        """Find all runs that support a certain result."""
        candidates = self._db.select('run', calculation=calc_id)
        def is_supporter(run):
            created = self._db.select('created', run=run['id'], path=path)
            assert len(created) == 1
            return created[0]['digest'] == digest

        return set(run['id'] for run in filter(is_supporter, candidates))


    def save_run(self, run, supporter_ids, outputs):
        """Save a Run.

        Args:
            run (dict): A row for the run table.
            supporter_ids (list): List of run ids supporting the input data.
            outputs (dict): Map of path: digest pairs produced by the run.
        """
        self._db.insert('run', **run)
        for sid in supporter_ids:
            self._db.insert('depended', run=run['id'], inputrun=sid)

        for path, digest in outputs.items():
            self._db.insert('created', run=run['id'], path=path, digest=digest)
            trust_row = dict(
                run=run['id'],
                path=path,
                usr=None,
                time=time.time(),
                correct=True)
            self._db.insert('trust', **trust_row)


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
        creations = self._db.select('created', digest=digest)
        run_ids = (row['run'] for row in creations)
        return set(run_ids)


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


class Graph(object):
    def __init__(self):
        super(Graph, self).__init__()
        self._graph = nx.DiGraph()
        self._tasks = set()
        self.outputs = set()

    def add_task(self, task):
        if any(output in self._graph for output in task.outputs):
            raise ValueError('duplicate outputs not allowed')

        for inp in task.inputs:
            self._graph.add_edge(inp, task)

        for output in task.outputs:
            self._graph.add_edge(task, output)

        self.outputs.update(task.outputs)
        self._tasks.add(task)

    def get_tasks(self, *requested_outputs):
        ancestors = set.union(*(nx.ancestors(self._graph, output) for output
                                in requested_outputs))
        ancestor_graph = nx.subgraph(self._graph, ancestors)
        tasks_in_subgraph = self._tasks.intersection(ancestors)
        task_graph = nx.projected_graph(ancestor_graph, tasks_in_subgraph)
        return(nx.topological_sort(task_graph))

    def predecessors(self, path):
        return(self._graph.predecessors(path))

    def ensure_complete(self):
        ## find input at start of graph
        ## add CopyTask for each such input
        for node in nx.nodes(self._graph):
            if len(nx.ancestors(self._graph, node)) == 0:
                if not isinstance(node, Task):
                    t = CopyTask(node)
                    self._graph.add_edge(t, node)
                    self.outputs.update(node)
                    self._tasks.add(t)


class Runner(object):
    def __init__(self, log, storage, graph):
        super(Runner, self).__init__()
        self.log = log
        self.storage = storage
        self._graph = graph
        self._fsos = dict()

    def _calc_id(self, task):
        # Note: id_contents should be independent of the order of inputs
        id_contents = [task.id, {path: self._fsos[path] for path in task.inputs}]
        return hexdigest(unique_json(id_contents))


    def ensure_exists(self, *requested_outputs):
        for task in self._graph.get_tasks(*requested_outputs):
            calc_id = self._calc_id(task)
            inputs = {path: self._fsos[path] for path in task.inputs}
            if not all(self.log.find_output(calc_id, path) for path in task.outputs):
                # TODO be more conservative: we only need to know the digests of the 
                # outputs actually used further down in the graph.
                self._run(task)
            else:
                logger.info('Calculation {} has been run previously'.format(calc_id))
           
            for path in task.outputs:
                # TODO likewise, be more conservative here too of course
                digest = self.log.find_output(calc_id, path)
                self._fsos[path] = digest
                if path in requested_outputs and not self.storage.has_file(digest):
                    self._run(task)


    def _run(self, task):
        inputs = {path: self._fsos[path] for path in task.inputs}
        calc_id = self._calc_id(task)

        supporter_ids = set()
        for path, digest in inputs.items():
            # Find supporters (for provenance)
            parent_task, = self._graph.predecessors(path)
            parent_calc_id = self._calc_id(parent_task)
            supporter_ids.update(self.log.find_supporters(parent_calc_id, path, digest))
            
            # Make sure input files are available
            if not self.storage.has_file(digest):
                parent_task, = self._graph.predecessors(path)
                self._run(parent_task)

        logger.info('Running calculation {}'.format(calc_id))

        # create temp dir
        # run task.prepare(...)
        # copy input files
        # invoke task
        # archive input files
        # save run info in log

        workdir = os.path.join('/tmp/.gpc/', calc_id)
        if os.path.exists(workdir):
            logger.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)
        logger.debug('creating {}'.format(workdir))
        os.makedirs(workdir)

        for path, digest in inputs.items():
            self.storage.copy_to(digest, os.path.join(workdir, path))
       
        logger.debug('running in {}'.format(workdir))
        task.run(workdir)

        outputs = {}
        for path in task.outputs:
            dst_path = os.path.join(workdir, path)
            digest = self.storage.save(dst_path)
            outputs[path] = digest

        run = dict(
            id=str(uuid4()),
            usr=None,
            calculation=calc_id,
            info=None,
            time=time.time())

        self.log.save_run(run, supporter_ids, outputs)
        
        if os.path.exists(workdir):
            logger.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)

    def make(self, output):
        self._graph.ensure_complete()
        self.ensure_exists(output)
        self.storage.copy_to(self._fsos[output], output)

class Task(object):
    def __init__(self, command):
        super(Task, self).__init__()
        self.command = command
        self.inputs = []
        self.outputs = []
        
    def run(self, workdir):
        original_wd = os.getcwd()
        os.chdir(workdir)
        try:
            subprocess.call(self.command, shell=True)
        except Exception as e:
            raise e
        finally:
            os.chdir(original_wd)

    def __repr__(self):
        return '{} with command "{}"'.format(type(self), self.command)

class ShellTask(Task):
    """docstring for ShellTask"""
    def __init__(self, command, inputs, outputs):
        super(ShellTask, self).__init__(command)
        self.inputs = inputs
        self.outputs = outputs
        self.id = command

class CopyTask(Task):
    def __init__(self, outputs):
        original_wd = os.getcwd()
        files = [original_wd + '/' + o for o in outputs]
        command = "cp " + ' '.join(files) + ' ./'
        super(CopyTask, self).__init__(command)
        self.inputs = []
        self.outputs = outputs
        self.id = self.command
        
# def print_run(run, level=1):
#     spacing = '   ' * level
#     def pr(val):
#         print('%s%s' % (spacing, val))
#     pr(run)
#     pr(run.calculation)
#     pr(run.info)
#     for path, digest in run.calculation.input_fsos.items():
#         pr(' Input %s (%s)' % (path, digest))
#         for i, sr in enumerate(r for r in run.supporters if path in r.output_fsos):
#                 pr('  Supporter #%i' % (i+1))
#                 print_run(sr, level=level+1)
#     pr('')
