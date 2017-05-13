import shutil
import subprocess
import os
import uuid
import itertools
from collections import defaultdict
import time
import tempfile
import logging
import attr
import boyle
from boyle import ConflictException, NotFoundException

logger = logging.getLogger(__name__)

def _run_op(op, work_dir):
    prev_dir = os.getcwd()
    os.chdir(work_dir)
    print(f'doing "{op}" in {work_dir}')
    subprocess.run(op, shell=True)
    os.chdir(prev_dir)


@attr.s
class Scheduler:

    log = attr.ib()
    storage = attr.ib()
    project_dir = attr.ib()
    work_base_dir = attr.ib()
    outdir = attr.ib()
    user = attr.ib()

    def __attrs_post_init__(self):
        self.project_dir = os.path.abspath(self.project_dir)
        self.work_base_dir = os.path.abspath(self.work_base_dir)
        self.outdir = os.path.abspath(self.outdir)
        os.makedirs(self.work_base_dir, exist_ok=True)

    def _determine_sets(self, comps):
        comps = boyle.Comp.topological_sort(comps)

        sets = {
            name: set() for name in (
                'Concrete',
                'Abstract',
                'Known',
                'Unknown',
                'Conflict',
                'Restorable',
                'Runnable',
                )
            }

        for comp in comps:
            if set(comp.parents) <= sets['Known']:
                sets['Concrete'].add(comp)
            else:
                sets['Abstract'].add(comp)
                continue

            if set(comp.parents) <= sets['Restorable']:
                sets['Runnable'].add(comp)

            calc = self.log.get_calculation(comp, self.user)
            try:
                resource = self.log.get_result(calc, comp.loc, self.user)
                sets['Known'].add(comp)
            except ConflictException as e:
                sets['Conflict'].add(comp)
            except NotFoundException as e:
                sets['Unknown'].add(comp)

            if (
                comp in sets['Known']
                and self.storage.can_restore(resource, self.work_base_dir)):
                sets['Restorable'].add(comp)

        return sets

    def _get_ready_and_needed(self, requested):
        requested = set(requested)
        logger.debug(f'Requested: {requested}')
        assert len(requested) > 0
        comps = boyle.Comp.get_ancestors(requested)
        sets = self._determine_sets(comps)

        if sets['Conflict']:
            raise ConflictError(sets['Conflict'])

        if requested <= sets['Restorable']:
            return set()

        candidates = set()
        additional = (requested - sets['Restorable']).union(sets['Unknown'])

        while additional:
            candidates.update(additional)
            additional = (
                set(itertools.chain(*(comp.parents for comp in additional)))
                - sets['Restorable']
                )

        final = candidates.intersection(sets['Runnable'])
        assert len(final) > 0
        logging.debug(f'Ready and needed: {final}')
        return final

    def _ensure_available(self, requested):
        while True:
            comps_to_run = self._get_ready_and_needed(requested)
            if not comps_to_run:
                break

            comps_by_calc = defaultdict(set)
            for comp in comps_to_run:
                calc = self.log.get_calculation(comp, self.user)
                comps_by_calc[calc].add(comp)

            for calc, comps in comps_by_calc.items():
                self._run_calc(calc, [comp.loc for comp in comps])


    def _run_calc(self, calc, out_locs):
        with tempfile.TemporaryDirectory(
                prefix=calc.calc_id,
                dir=self.work_base_dir) as work_dir:

            out_paths = {loc: os.path.join(work_dir, loc) for loc in out_locs}

            for inp in calc.inputs:
                self.storage.restore(inp, work_dir)

            start_time = time.time()
            _run_op(calc.op, work_dir)
            end_time = time.time()

            results = tuple(
                boyle.Resource(loc, boyle.core.digest_file(path))
                for loc, path in out_paths.items())

            for res in results:
                self.storage.store(res, work_dir)

            run = boyle.Run(
                run_id=str(uuid.uuid4()),
                calc=calc,
                start_time=start_time,
                end_time=end_time,
                results=results,
                user=self.user
                )

            self.log.save_run(run)

    def make(self, requested_comps):
        self._ensure_available(requested_comps)
        resources = set()
        for comp in requested_comps:
            calc = self.log.get_calculation(comp, self.user)
            result = self.log.get_result(calc, comp.loc, self.user)
            resources.add(result)

        for res in resources:
            self.storage.restore(res, self.outdir)
