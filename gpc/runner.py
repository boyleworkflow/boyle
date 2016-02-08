import enum
import logging
import os
from gpc import get_calc_id, get_comp_id, NotFoundException
import time
from uuid import uuid4
import shutil

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@enum.unique
class Status(enum.IntEnum):
    inputs_unknown = 0
    inputs_missing = 1
    ready_to_run = 2
    digest_known = 3
    file_exists = 4


class Runner(object):
    def __init__(self, log, storage, graph):
        super(Runner, self).__init__()
        self.log = log
        self.storage = storage
        self.graph = graph


    def _get_calc_id(self, task):
        logger.debug('Getting calc_id\n\ttask: {}'.format(task))
        inp_tasks = {path: self.graph.get_task(path) for path in task.inputs}
        inp_calc_ids = {p: self._get_calc_id(t) for p, t in inp_tasks.items()}
        inp_digests = {p: self.log.get_digest(calc_id, p)
                        for p, calc_id in inp_calc_ids.items()}

        calc_id = get_calc_id(task, inp_digests)

        logger.debug('calc_id decided:\n\ttask: {}\n\tcalc_id: {}'.format(task, calc_id))

        self.log.save_task(
            task_id=task.id,
            definition='changeme',
            sysstate='changeme')

        self.log.save_calculation(
            calc_id=calc_id,
            task_id=task.id,
            inputs=inp_digests)

        return calc_id


    def _get_comp_id(self, task):
        input_tasks = [self.graph.get_task(path) for path in task.inputs]
        subcomp_ids = [self._get_comp_id(task) for task in input_tasks]

        calc_id = self._get_calc_id(task)
        comp_id = get_comp_id(calc_id, subcomp_ids)

        self.log.save_composition(
            comp_id=comp_id,
            calc_id=calc_id,
            subcomp_ids=subcomp_ids)

        return comp_id


    def _save_request(self, path, request_time):
            task = self.graph.get_task(path)
            comp_id = self._get_comp_id(task)
            digest = self.get_digest(path)

            self.log.save_request(
                comp_id=comp_id,
                time=request_time,
                digest=digest,
                path=path)


    def get_digest(self, path):
        task = self.graph.get_task(path)
        calc_id = self._get_calc_id(task)
        digest = self.log.get_digest(calc_id, path)
        return digest


    def get_status(self, path):
        task = self.graph.get_task(path)
        input_statuses = [self.get_status(p) for p in task.inputs]

        if any((s < Status.digest_known for s in input_statuses)):
            return Status.inputs_unknown

        calc_id = self._get_calc_id(task)

        try:
            digest = self.log.get_digest(calc_id, path)
        except NotFoundException:
            # Output digest does not exist
            if all((s == Status.file_exists for s in input_statuses)):
                return Status.ready_to_run
            else:
                return Status.inputs_missing

        # If we come this far, the digests exists
        if self.storage.has_file(digest):
            return Status.file_exists
        else:
            return Status.digest_known


    def ensure_exists(self, *requested_paths):
        request_time = time.time()

        self.graph.ensure_complete()

        upstream_paths = self.graph.get_upstream_paths(*requested_paths)
        logger.debug('Requested: {}'.format(requested_paths))
        logger.debug('Upstream paths: {}'.format(upstream_paths))

        status_goals = {p: Status.digest_known for p in upstream_paths}
        status_goals.update({p: Status.file_exists for p in requested_paths})

        counter = 0
        work_remains = True
        while work_remains:
            work_remains = False # Let's assume until we are proven wrong.

            counter += 1
            logger.info('Working round {}'.format(counter))

            for path in upstream_paths:
                task = self.graph.get_task(path)
                status = self.get_status(path)
                if status.value >= status_goals[path].value:
                    continue
                else:
                    work_remains = True

                logger.info('Working on {} ({})'.format(path, status))

                # Working on something with missing inputs means
                # that the inputs must all exist before continuing:
                if status == Status.inputs_missing:
                    status_goals.update(
                        {p: Status.file_exists for p in task.inputs})

                # Working on something "ready to run" or "digest known"
                # means that it should be run / rerun
                elif status in (Status.ready_to_run, Status.digest_known):
                    self._run(task)

                else:
                    raise RuntimeError(
                        'Unexpected status {} for {}'.format(status, path))

                # Always break (start a new round) after doing something
                break

        for path in requested_paths:
            self._save_request(path, request_time)

    def _run(self, task):
        calc_id = self._get_calc_id(task)
        logger.info((
            'Running calculation\n'
            '\tcalc_id: {}\n'
            '\ttask: {}').format(calc_id, task))

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
