from enum import Enum
import logging
import os
from gpc import get_calc_id, get_comp_id, NotFoundException
import time
from uuid import uuid4
import shutil

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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


    def get_calc_id(self, task):
        logger.debug('Getting calc_id\n\ttask: {}'.format(task))
        inp_tasks = {path: self.graph.get_task(path) for path in task.inputs}
        inp_calc_ids = {p: self.get_calc_id(t) for p, t in inp_tasks.items()}
        inp_digests = {p: self.log.get_digest(calc_id, p)
                        for p, calc_id in inp_calc_ids.items()}

        calc_id = get_calc_id(task, inp_digests)

        self.log.save_task(
            task_id=task.id,
            definition='changeme',
            sysstate='changeme')

        self.log.save_calculation(
            calc_id=calc_id,
            task_id=task.id,
            inputs=inp_digests)

        logger.debug('calc_id decided:\n\ttask: {}\n\tcalc_id: {}'.format(task, calc_id))

        return calc_id


    def get_comp_id(self, task):
        input_tasks = [self.graph.get_task(path) for path in task.inputs]
        subcomp_ids = [self.get_comp_id(task) for task in input_tasks]

        calc_id = self.get_calc_id(task)
        comp_id = get_comp_id(calc_id, subcomp_ids)

        self.log.save_composition(
            comp_id=comp_id,
            calc_id=calc_id,
            subcomp_ids=subcomp_ids)

        return comp_id


    def get_digest(self, path):
        task = self.graph.get_task(path)
        calc_id = self.get_calc_id(task)
        digest = self.log.get_digest(calc_id, path)
        return digest


    def get_status(self, path):
        task = self.graph.get_task(path)
        try:
            calc_id = self.get_calc_id(task)
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


    def _save_request(self, path, digest):
        task = self.graph.get_task(path)
        comp_id = self.get_comp_id(task)
        self.log.save_request(
            comp_id=comp_id,
            time=time.time(),
            digest=digest,
            path=path)


    def ensure_exists(self, *requested_paths):
        self.graph.ensure_complete()

        upstream_paths = self.graph.get_upstream_paths(*requested_paths)
        logger.debug('Requested: {}'.format(requested_paths))
        logger.debug('Upstream paths: {}'.format(upstream_paths))

        status_goals = {p: Status.digest_known for p in upstream_paths}
        status_goals.update({p: Status.file_exists for p in requested_paths})

        def get_paths_to_work_on():
            counter = 0
            while True:
                counter += 1
                logger.info('working round {}'.format(counter))
                done = True
                for path in upstream_paths:
                    status = self.get_status(path)
                    logger.info('working {} {}'.format(path, status))
                    if status.value < status_goals[path].value:
                        logger.info('working on {} {}'.format(path, status))
                        yield path, status
                        done = False
                        break
                logger.info('end of working round {}, done: {}'.format(counter, done))
                if done:
                    break

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

        for path in requested_paths:
            digest = self.get_digest(path)
            self._save_request(path, digest)

    def _run(self, task):
        calc_id = self.get_calc_id(task)
        logger.info('Running calculation\n\tcalc_id: {}\n\ttask: {}'.format(calc_id, task))

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

        inputs = {path: self.get_digest(path) for path in task.inputs}
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
            run_id=str(uuid4()),
            info='changeme',
            time=time.time(),
            calc_id=calc_id,
            digests=digests
            )

        if os.path.exists(workdir):
            logger.debug('deleting {}'.format(workdir))
            shutil.rmtree(workdir)

    def make(self, output):
        self.ensure_exists(output)
        digest = self.get_digest(output)
        self.storage.copy_to(digest, output)
