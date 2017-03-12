import tempfile
import time

import attr

@attr.s
class ConflictError(Exception):
    defs = attr.ib()

@attr.s
class Instrument:
    def copy(self, src_path, dst_path): pass
    def digest(self, path): pass # return Digest or raise exception
    def can_save(self, path, storage): pass # return boolean
    def save(self, path, storage): pass # return StorageMeta or raise exception
    def can_restore(self, meta, storage): pass # return boolean
    def restore(self, meta, storage, path): pass

@attr.s
class Definition:
    instrument = attr.ib()
    parents = attr.ib()
    operation = attr.ib()

@attr.s
class Resource:
    instrument = attr.ib()
    digest = attr.ib()

@attr.s
class Operation:
    pass

@attr.s
class Calculation:
    inputs = attr.ib()
    operation = attr.ib()


@attr.s
class Run:
    calculation = attr.ib()
    results = attr.ib()
    start_time = attr.ib()
    end_time = attr.ib()

    def __attrs_post_init__(self):
        self.unique_str = uuid.uuid4()


def _get_ancestors(defs):
    defs = set()
    new_defs = set(defs)
    while new_defs:
        defs.update(new_defs)
        new_defs = set.union(*(d.parents for d in new_defs)) - defs
    return defs

def digest_str(s):
    hashlib.sha1(s.encode('utf-8')).hexdigest()

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)

def get_id_str(obj):
    id_obj = {
        'type': type(object).__qualname__,
        'unique_str': object.unique_str
        }
    return digest_str(unique_json(id_obj))


@attr.s
class Scheduler:

    log = attr.ib()
    storage = attr.ib()
    project_dir = attr.ib()
    work_base_dir = attr.ib()

    def __attrs_post_init__(self):
        self.tempdir = tempfile.mkdtemp(prefix='temp', dir=self.work_base_dir)

    def _has_stored_copy(self, resource):
        try:
            storage_meta = self.log.get_storage_meta(resource)
        except NotFound:
            return False
        return resource.instrument.can_restore(storage_meta, self.storage)

    def _resource_temp_dir(self, resource):
        return os.path.join(self.tempdir, get_id_str(resource))

    def _can_place(self, resource):
        return self._has_temp_copy(resource) or self._has_stored_copy(resource)

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
        defs = topological_sort(defs)

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
            if d.parents <= sets['Known']:
                sets['Concrete'].add(d)
            else:
                sets['Abstract'].add(d)
                continue

            if d.parents <= sets['Restorable']:
                sets['Runnable'].add(d)

            trusted_results = self.log.get_trusted_results(d)
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
        defs = _get_ancestors(requested)
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
                set.union(*(d.parents for d in candidates))
                - sets['Restorable']
                )

        final = candidates.intersection(sets['Runnable'])
        assert len(final) > 0
        return final

    def _ensure_available(requested):
        while True:
            defs_to_run = self._get_ready_and_needed(requested)
            if not defs_to_run:
                break

            defs_by_calc = defaultdict(set)
            for d in defs_to_run:
                calc = self.log.get_calculation(d)
                defs_by_calc[calc].add(d)

            for calc, instruments in defs_by_calc.items():
                self._run_calc(calc, instruments)


    def _run_calc(self, calc, instruments):
        with tempdir.TemporaryDirectory(
                prefix=get_id_str(calc),
                dir=self.work_base_dir) as work_dir:
            
            for inp in calc.inputs:
                self._place_resource(inp, work_dir)
            
            start_time = time.time()
            calc.operation.run(work_dir)
            end_time = time.time()

            results = [
                Resource(instrument, instrument.digest(work_dir))
                for instrument in instruments]

            for res in results:
                if not self._has_temp_copy(res):
                    instrument.copy(work_dir, self._resource_temp_dir(res))

            run = Run(
                calculation=calc,
                start_time=start_time,
                end_time=end_time,
                results=results,
                )

            self.log.save_run(run)
            







class Log:

    def get_trusted_results(calculation, instrument):
        raise NotImplementedError()

    def get_storage_meta(resource):
        raise NotImplementedError()


