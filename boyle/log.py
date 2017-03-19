import os
import sqlite3
import logging
import pkg_resources

logger = logging.getLogger(__name__)

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