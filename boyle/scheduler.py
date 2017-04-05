import attr
import boyle
import shutil

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

    def _determine_sets(self, defs):
        defs = boyle.Definition._topological_sort(defs)

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
            try:
                resource = self.log.get_result(calc, d.uri, self.user)
                sets['Known'].add(d)
            except ConflictException as e:
                sets['Conflict'].add(d)
            except NotFoundException as e:
                sets['Unknown'].add(d)

            if (
                d in sets['Known']
                and self.storage.can_restore(resource, self.work_base_dir)):
                sets['Restorable'].add(d)

        return sets

    def _get_ready_and_needed(self, requested):
        requested = set(requested)
        assert len(requested) > 0
        defs = boyle.Definition._get_ancestors(requested) | requested
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
                self._run_calc(calc, [d.uri for d in defs])


    def _run_calc(self, calc, uris):
        with tempfile.TemporaryDirectory(
                prefix=id_str(calc),
                dir=self.work_base_dir) as work_dir:

            uris = [os.path.join(work_dir, uri) for uri in uris]

            for inp in calc.inputs:
                self.storage.restore(inp, work_dir)

            start_time = time.time()
            calc.task.run(work_dir)
            print(work_dir)
            print(os.listdir(work_dir))
            end_time = time.time()

            results = [
                Resource(uri, boyle.core.digest_file(uri))
                for uri in uris]

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
            results = self.log.get_trusted_results(calc, d.uri)
            assert len(results) == 1
            resources.update(results)

        for res in resources:
            self.storage.restore(res, self.outdir)
