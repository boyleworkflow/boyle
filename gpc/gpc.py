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
from enum import Enum
from contextlib import suppress
import sqlite3
from collections import defaultdict

from gpc.common import hexdigest, digest_file, unique_json
from gpc import fsdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_ID = 1234
USER_NAME = 'aoeu'

class NotFoundException(Exception): pass


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
            with self._db:
                self._db.execute(
                    'insert into usr(id, name) values(?, ?)',
                    (USER_ID, USER_NAME))

    def save_calculation(self, calc_id=None, task=None, inputs=None):
        with suppress(sqlite3.IntegrityError), self._db as db:
            db.execute(
                'INSERT OR ABORT INTO calculation(id, task) values(?, ?)',
                (calc_id, task))

            db.executemany(
                'INSERT INTO uses (calculation, path, digest) '
                'values (?, ?, ?)',
                [(calc_id, p, d) for p, d in inputs.items()])


    def save_run(self, run_id=None, info=None, time=None,
                 calculation=None, digests=None):

        with self._db as db:
            db.execute(
                'INSERT INTO run(id, usr, info, time, calculation) '
                'values(?, ?, ?, ?, ?)',
                (run_id, USER_ID, info, time, calculation))

            db.executemany(
                'INSERT INTO created(run, path, digest) values(?, ?, ?)',
                [(run_id, p, d) for p, d in digests.items()])


    def get_digest(self, calc_id, path):
        """Try to find the digest of a calculation output.

        Args:
            calc_id (str): The id of the calculation in question.
            path (str): The relative path of the output.

        Returns:
            None: If the output cannot be found.
            str: A file digest, if exactly one is found.

        Raises:
            NotFoundException: If there is no candidate value.
            ConflictException: If there are more than one candidate values.
        """

        candidates = self._db.execute(
            'SELECT DISTINCT digest, trust.usr, correct FROM created '
            'INNER JOIN run ON created.run = run.id '
            'LEFT OUTER JOIN trust USING (calculation, digest, path) '
            'WHERE (path = ? AND calculation = ?)',
            (path, calc_id))

        opinions = defaultdict(lambda: defaultdict(list))
        for digest, usr, correct in candidates:
            opinions[digest][correct].append(usr)

        # If there are any digests which the current user does not
        # explicitly think anything about, and for which other users
        # have mixed opinions, there is a conflict:

        logger.debug('get_digest: path {} in calc {}'.format(path, calc_id))
        logger.debug('opinions: {}'.format(dict(opinions)))

        for digest, users in opinions.items():
            if (
                USER_ID not in users[True] + users[False] and
                len(users[True]) > 0 and
                len(users[False]) > 0):
                raise ConflictException(calc_id, path)


        # Remove all digests which noone trusts and at least one distrusts
        opinions = {
            digest: users for digest, users in opinions.items()
            if not (len(users[True]) == 0 and len(users[False]) > 0)}

        # Remove all digests the current user distrusts
        opinions = {
            digest: users for digest, users in opinions.items()
            if not USER_ID in users[False]}

        # If there is no digest left, nothing is found.
        # If there is exactly one left, it can be used.
        # If there is more than one left, there is a conflict.

        if len(opinions) == 0:
            raise NotFoundException()
        elif len(opinions) == 1:
            digest, = opinions.keys()
            return digest
        else:
            raise ConflictException(calc_id, path)

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
        raise NotImplementedError()


    def get_provenance(self, digest):
        raise NotImplementedError()


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
        self._paths = set()

    def add_task(self, task):
        if any(path in self._graph for path in task.outputs):
            raise ValueError('duplicate outputs not allowed')

        for path in task.inputs:
            self._graph.add_edge(path, task)

        for path in task.outputs:
            self._graph.add_edge(task, path)

        self._paths.update(task.outputs)
        self._tasks.add(task)

    def get_task(self, output_path):
        task, = self._graph.predecessors(output_path)
        return task

    def get_upstream_paths(self, *requested_paths):
        ancestors = set.union(*(nx.ancestors(self._graph, path) for path
                                in requested_paths))
        ancestor_graph = nx.subgraph(self._graph, ancestors)
        paths_in_subgraph = self._paths.intersection(ancestors)
        path_graph = nx.projected_graph(ancestor_graph, paths_in_subgraph)
        return(nx.topological_sort(path_graph))

    def ensure_complete(self):
        ## find input at start of graph
        ## add CopyTask for each such input
        for node in nx.nodes(self._graph):
            if len(nx.ancestors(self._graph, node)) == 0:
                if not isinstance(node, Task):
                    t = CopyTask(node)
                    self._graph.add_edge(t, node)
                    self._paths.update(node)
                    self._tasks.add(t)

def calc_id(task, input_digests):
    """
    Compute the calculation id of a task with certain input digests.

    Args:
        task: The Task object in question.
        input_digests (dict-like): A mapping such that
            input_digests[path] == digest for all inputs used by the task.

    Returns:
        A string with the calculation id.

    """
    # Note: calc_id could be constructed in different ways, but
    # must be independent of the order of inputs.
    id_contents = [task.id, {p: input_digests[p] for p in task.inputs}]
    return hexdigest(unique_json(id_contents))

def comp_id(calc_id, input_comp_ids):
    """
    Compute the composition id of a calculation and its input compositions.

    Args:
        calc_id: The calculation id of the calculation.
        input_comp_ids (iterable): An iterable with the comp_id values
            of the inputs to the composition.

    Returns:
        A string with the composition id.

    """
    # Note: comp_id could be constructed in different ways, but
    # must be independent of the order of inputs.
    id_contents = [calc_id, list(sorted(input_comp_ids))]
    return hexdigest(unique_json(id_contents))

class Status(Enum):
    unknown = 0
    no_digest = 1
    digest_known = 2
    file_exists = 3

class Runner(object):
    def __init__(self, log, storage, graph):
        super(Runner, self).__init__()
        self.log = log
        self.storage = storage
        self.graph = graph


    def calc_id(self, task):
        input_tasks = {path: self.graph.get_task(path) for path in task.inputs}
        input_calc_ids = {p: self.calc_id(t) for p, t in input_tasks.items()}
        input_digests = {p: self.log.get_digest(calc_id, p)
                        for p, calc_id in input_calc_ids.items()}

        calc_id = calc_id(task, input_digests)

        self.log.save_calculation(
            id=calc_id,
            task=task.id,
            inputs=input_digests)

        return calc_id


    def comp_id(self, task):
        input_tasks = [self.graph.get_task(path) for path in task.inputs]
        input_comp_ids = [self.comp_id(task) for task in input_tasks]

        comp_id = comp_id(self.calc_id(task), input_comp_ids)

        self.log.save_composition(
            composition=comp_id,
            calculation=calc_id,
            inputs=input_comp_ids)

        return comp_id


    def get_status(self, path):
        task = self.graph.get_task(path)
        try:
            calc_id = self.calc_id(task)
        except NotFoundException:
            return Status.unknown

        try:
            digest = self.log.get_digest(calc_id, path)
        except NotFoundException:
            return Status.no_digest

        if self.storage.has_file(digest):
            return Status.file_exists
        else:
            return Status.digest_known


    def _save_request(self, path):
        task = self.graph.get_task(p)
        digest = self.log.get_digest(self.calc_id(task), path)
        comp_id = comp_id(task)
        self.log.save_request(
            composition=self.comp_id(task),
            time=time.time(),
            usr=USER_ID,
            digest=digest)


    def ensure_exists(self, *requested_paths):
        self.graph.ensure_complete()

        upstream_paths = self.graph.get_upstream_paths(*requested_paths)

        status_goals = {p: Status.digest_known for p in upstream_paths}
        status_goals.update({p: Status.file_exists for p in requested_paths})

        def get_paths_to_work_on():
            done = False
            while not done:
                for path in upstream_paths:
                    status = self.get_status(path)
                    if status < status_goals[p]:
                        yield path, status
                        break
                done = True

        for path, status in get_paths_to_work_on():
            task = self.graph.get_task(path)
            if status in (Status.no_digest, Status.digest_known):
                all_inputs_exist = True
                for p in task.inputs:
                    if self.get_status(p) != Status.file_exists:
                        all_inputs_exist = False
                        status_goals[p] = Status.file_exists

                if all_inputs_exist:
                    self._run(task)
            else:
                raise RuntimeError(
                    'Unexpected status {} for {}'.format(status, path))

        for p in upstream_paths:
            self._save_composition(p)

        for p in requested_paths:
            self._save_request(p)

    def _run(self, task):
        calc_id = self.calc_id(task)
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

        inputs = {p: self.log.get_digest(calc_id, p) for p in task.inputs}
        for path, digest in inputs.items():
            self.storage.copy_to(digest, os.path.join(workdir, path))
       
        logger.debug('running in {}'.format(workdir))
        task.run(workdir)

        digests = {}
        for path in task.outputs:
            dst_path = os.path.join(workdir, path)
            digest = self.storage.save(dst_path)
            digests[path] = digest

        self.log.save_run(
            id=str(uuid4()),
            info=info,
            time=time.time(),
            calculation=calc_id,
            digests=digests
            )

        if os.path.exists(workdir):
            logger.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)

    def make(self, output):
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