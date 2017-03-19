import os
import functools
import subprocess
import tempfile
import time
import json
import hashlib
import itertools
from collections import defaultdict
import shutil
import uuid

import attr

digest_func = hashlib.sha1

def digest_str(s):
    return digest_func(s.encode('utf-8')).hexdigest()

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)



def id_str(obj):
    id_obj = obj.__boyle_id__()
    try:
        json = unique_json(id_obj)
    except TypeError as e:
        msg = f'The __boyle_id__ of obj is not JSON serializable: {id_obj}'
        raise TypeError(msg) from e
    id_obj = {
        'type': type(obj).__qualname__,
        'obj': id_obj
        }
    return digest_str(json)
    

@attr.s
class ConflictError(Exception):
    defs = attr.ib()

class NotFoundError(Exception): pass

@attr.s
class Instrument:
    def copy(self, src_path, dst_path): pass
    def digest(self, path): pass # return Digest or raise exception
    def can_save(self, path, storage): pass # return boolean
    def save(self, path, storage): pass # return StorageMeta or raise exception
    def can_restore(self, meta, storage): pass # return boolean
    def restore(self, meta, storage, path): pass

def _get_ancestors(defs):
    defs = set()
    new_defs = set(defs)
    while new_defs:
        defs.update(new_defs)
        new_defs = set.union(*(d.parents for d in new_defs)) - defs
    return defs

@attr.s
class Definition:
    instrument = attr.ib()
    parents = attr.ib()
    operation = attr.ib()

    def __attrs_post_init__(self):
        self.parents = tuple(self.parents)

    def __lt__(self, other_def):
        my_ancestors = _get_ancestors([self])
        return other_def not in my_ancestors

    def __boyle_id__(self):
        return {
            'instrument': id_str(self.instrument),
            'parents': [id_str(p) for p in self.parents],
            'operation': id_str(self.operation)
        }

@attr.s
class Resource:
    instrument = attr.ib()
    digest = attr.ib()

    def __boyle_id__(self):
        return {
            'instrument': id_str(self.instrument),
            'digest': self.digest
        }

@attr.s
class Operation:
    pass

@attr.s
class Calculation:
    inputs = attr.ib()
    operation = attr.ib()

    def __attrs_post_init__(self):
        self.inputs = tuple(self.inputs)

    def __boyle_id__(self):
        return {
            'inputs': [id_str(inp) for inp in self.inputs],
            'operation': id_str(self.operation)
        }


@attr.s
class Run:
    run_id = attr.ib()
    calculation = attr.ib()
    results = attr.ib()
    start_time = attr.ib()
    end_time = attr.ib()

    def __attrs_post_init__(self):
        self.results = tuple(self.results)

    def __boyle_id__(self):
        return {'run_id': self.run_id}





def _topological_sort(defs):
    defs = list(defs)
    defs.sort()
    return defs



@attr.s
class Scheduler:

    log = attr.ib()
    storage = attr.ib()
    project_dir = attr.ib()
    work_base_dir = attr.ib()
    outdir = attr.ib()

    def __attrs_post_init__(self):
        self.project_dir = os.path.abspath(self.project_dir)
        self.work_base_dir = os.path.abspath(self.work_base_dir)
        self.outdir = os.path.abspath(self.outdir)
        self.tempdir = tempfile.mkdtemp(prefix='temp', dir=self.work_base_dir)

    def _has_stored_copy(self, resource):
        try:
            storage_meta = self.log.get_storage_meta(resource)
        except NotFoundError:
            return False
        return resource.instrument.can_restore(storage_meta, self.storage)

    def _resource_temp_dir(self, resource):
        return os.path.join(self.tempdir, id_str(resource))

    def _can_place(self, resource):
        return self._has_temp_copy(resource) or self._has_stored_copy(resource)

    def _create_resource_temp_dir(self, resource):
        path = self._resource_temp_dir(resource)
        assert not os.path.exists(path)
        os.mkdir(path)

    def _has_temp_copy(self, resource):
        temp_src_dir = self._resource_temp_dir(resource)
        return os.path.exists(temp_src_dir)

    def _place_resource(self, resource, dst_dir):
        if self._has_temp_copy(resource):
            temp_src_dir = self._resource_temp_dir(resource)
            resource.instrument.copy(temp_src_dir, dst_dir)

        else:
            storage_meta = self.log.get_storage_meta(resource)
            resource.instrument.restore(storage_meta, self.storage, dst_dir)

        assert resource.instrument.digest(dst_dir) == resource.digest


    def _determine_sets(self, defs):
        defs = _topological_sort(defs)

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

        for d in defs:
            if set(d.parents) <= sets['Known']:
                sets['Concrete'].add(d)
            else:
                sets['Abstract'].add(d)
                continue

            if set(d.parents) <= sets['Restorable']:
                sets['Runnable'].add(d)

            calc = self.log.get_calculation(d)
            trusted_results = self.log.get_trusted_results(calc, d.instrument)
            num_trusted = len(trusted_results)

            if num_trusted == 0:
                sets['Unknown'].add(d)
                continue
            elif num_trusted > 2:
                sets['Conflict'].add(d)
                continue
            elif num_trusted == 1:
                sets['Known'].add(d)
                trusted_result, = trusted_results

            if self._can_place(trusted_result):
                sets['Restorable'].add(d)

        return sets
    
    def _get_ready_and_needed(self, requested):
        requested = set(requested)
        assert len(requested) > 0
        defs = _get_ancestors(requested) | requested
        sets = self._determine_sets(defs)

        if sets['Conflict']:
            raise ConflictError(sets['Conflict'])

        if requested <= sets['Restorable']:
            return set()

        candidates = set()
        additional = (requested - sets['Restorable']).union(sets['Unknown'])

        while additional:
            candidates.update(additional)
            additional = (
                set(itertools.chain(*(d.parents for d in candidates)))
                - sets['Restorable']
                )

        final = candidates.intersection(sets['Runnable'])
        assert len(final) > 0
        return final

    def _ensure_available(self, requested):
        while True:
            defs_to_run = self._get_ready_and_needed(requested)
            if not defs_to_run:
                break

            defs_by_calc = defaultdict(set)
            for d in defs_to_run:
                calc = self.log.get_calculation(d)
                defs_by_calc[calc].add(d)

            for calc, defs in defs_by_calc.items():
                self._run_calc(calc, [d.instrument for d in defs])


    def _run_calc(self, calc, instruments):
        with tempfile.TemporaryDirectory(
                prefix=id_str(calc),
                dir=self.work_base_dir) as work_dir:
            
            for inp in calc.inputs:
                self._place_resource(inp, work_dir)
            
            start_time = time.time()
            calc.operation.run(work_dir)
            print(work_dir)
            print(os.listdir(work_dir))
            end_time = time.time()

            results = [
                Resource(instrument, instrument.digest(work_dir))
                for instrument in instruments]

            for res in results:
                if not self._has_temp_copy(res):
                    self._create_resource_temp_dir(res)
                    res.instrument.copy(work_dir, self._resource_temp_dir(res))

            run = Run(
                run_id=str(uuid.uuid4()),
                calculation=calc,
                start_time=start_time,
                end_time=end_time,
                results=results,
                )

            self.log.save_run(run)

    def make(self, requested):
        self._ensure_available(requested)
        resources = set()
        for d in requested:
            calc = self.log.get_calculation(d)
            results = self.log.get_trusted_results(calc, d.instrument)
            assert len(results) == 1
            resources.update(results)
        
        for res in resources:
            self._place_resource(res, self.outdir)


class Log:

    def __init__(self, path):
        self._path = path
        with open(path, 'r') as f:
            self._data = json.load(f)
        if not 'runs' in self._data:
            self._data['runs'] = {}

    def save(self):
        with open(self._path, 'w') as f:
            json.dump(self._data, f)

    def get_trusted_results(self, calculation, instrument):
        code = id_str(calculation) + id_str(instrument)
        runs = self._data['runs'].values()
        instr_id = id_str(instrument)
        calc_id = id_str(calculation)
        relevant_runs = (r for r in runs if r['calc_id'] == calc_id)
        
        digests = set()
        for run in relevant_runs:
            digests.update(
                res['digest'] for res in run['results']
                if res['instr_id'] == instr_id
                )

        results = [Resource(instrument=instrument, digest=d) for d in digests]

        return results

    def save_run(self, run):
        assert run.run_id not in self._data['runs']
        self._data['runs'][run.run_id] = {
            'calc_id': id_str(run.calculation),
            'results': [
                {'instr_id': id_str(res.instrument), 'digest': res.digest}
                for res in run.results
                ]
            }

    def get_unique_trusted_result(self, calculation, instrument):
        results = self.get_trusted_results(calculation, instrument)
        try:
            result, = results
            return result
        except ValueError as e:
            num = len(results)
            raise f'Found {num} results; expected exactly 1'

    def get_calculation(self, d):
        inputs = [
            get_unique_trusted_result(self.get_calculation(p), p.instrument)
            for p in d.parents
            ]
        return Calculation(inputs=inputs, operation=d.operation)


    def get_storage_meta(self, resource):
        raise NotImplementedError()

@attr.s
class Shell:

    cmd = attr.ib()

    def __boyle_id__(self):
        return {'cmd': self.cmd}

    def run(self, work_dir):
        print(f'running "{self.cmd}" in {work_dir}')
        proc = subprocess.Popen(self.cmd, cwd=work_dir, shell=True)
        proc.wait()

@attr.s
class File:

    path = attr.ib()
    def __boyle_id__(self):
        return {'path': self.path}

    def digest(self, work_dir):
        path = os.path.join(work_dir, self.path)
        with open(path, 'rb') as f:
            return digest_func(f.read()).hexdigest()

    def copy(self, src_dir, dst_dir):
        src_path = os.path.join(src_dir, self.path)
        dst_path = os.path.join(dst_dir, self.path)
        shutil.copy2(src_path, dst_path)

if __name__ == '__main__':
    op = Shell('ls > abc')

    d = Definition(File('abc'), (), op)
    
    log = Log('db.json')
    s = Scheduler(
        log=log,
        storage=None,
        project_dir='project',
        work_base_dir='worktemp',
        outdir='project/outdir'
        )
    s.make([d])
    log.save()