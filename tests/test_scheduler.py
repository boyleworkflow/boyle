import unittest
import tempfile
import os
from itertools import product, combinations_with_replacement
import boyle
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def _make(requested_comps, outdir):
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir = 'here'
        os.mkdir(tempdir)
        log_path = os.path.join(tempdir, 'log.db')
        storage_dir = os.path.join(tempdir, 'storage')
        work_dir = os.path.join(tempdir, 'work')
        project_dir = os.path.join(tempdir, 'project')
        scheduler = boyle.Scheduler(
            log=boyle.Log(log_path),
            storage=boyle.Storage(storage_dir),
            project_dir=project_dir,
            work_base_dir=work_dir,
            outdir=outdir,
            user=boyle.User(user_id='uid', name='user name')
            )

        scheduler.make(requested_comps)


class TestScheduler(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple_1(self):
        a = boyle.Comp(parents=(), op='echo hello > file_a', loc='file_a')
        b = boyle.Comp(parents=(), op='echo world > file_b', loc='file_b')
        c = boyle.Comp(
            parents=(a, b),
            op='cat file_a file_b > file_c',
            loc='file_c')

        with tempfile.TemporaryDirectory() as outdir:
            _make([c], outdir)

            with open(os.path.join(outdir, 'file_c'), 'r') as f:
                logger.debug(f.read())



if __name__ == '__main__':
    unittest.main()
