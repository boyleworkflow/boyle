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
class Result:
    calculation = attr.ib()
    resource = attr.ib()


def _get_ancestors(defs):
    defs = set()
    new_defs = set(defs)
    while new_defs:
        defs.update(new_defs)
        new_defs = set.union(*(d.parents for d in new_defs)) - defs
    return defs


@attr.s
class Scheduler:

    log = attr.ib()
    storage = attr.ib()

    def _can_restore(resource):
        try:
            storage_meta = self.log.get_storage_meta(resource)
        except NotFound:
            return False
        return resource.instrument.can_restore(storage_meta, self.storage)

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

            if self._can_restore(trusted_result.resource):
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

    def _ensure_restorable(requested, path, log):
        while True:
            defs_to_run = self._get_ready_and_needed(requested)
            if not defs_to_run:
                break

            for d in defs_to_run:
                run_def(d)

        

class Log:

    def get_trusted_results(calculation, instrument):
        raise NotImplementedError()

    def get_storage_meta(resource):
        raise NotImplementedError()


