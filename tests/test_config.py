import unittest
import gpc
import os
import tempfile
import shutil
import yaml
from subprocess import call, check_output

NAME = 'myname'
ID = 'xyz123'

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.origdir = os.getcwd()
        self.tempdir = tempfile.mkdtemp()
        self.temp_global_backup = os.path.join(self.tempdir, 'global_config')
        shutil.move(gpc.config.GLOBAL_PATH, self.temp_global_backup)
        os.chdir(self.tempdir)

        call(['gpc', 'config', 'set', '--global', 'user.name', NAME])
        call(['gpc', 'config', 'set', '--global', 'user.id', ID])

    def test_priority(self):        
        conf = gpc.config.load()

        # From global file
        self.assertTrue(conf['user.name'] == NAME)
        self.assertTrue(conf['user.id'] == ID)

        # No local file exists
        self.assertTrue(not os.path.exists(gpc.config.LOCAL_PATH))

        # Create local
        call(['gpc', 'config', 'set', '--local', 'user.name', NAME + 'local'])
        self.assertTrue(os.path.exists(gpc.config.LOCAL_PATH))

        # Check that local file overrides
        conf = gpc.config.load()
        self.assertTrue(conf['user.name'] == NAME + 'local')
        self.assertTrue(conf['user.id'] == ID)

    def test_complex(self):
        complex_value = dict(abc=1, cde=["str", 123])
        key = 'abc'
        gpc.config.set('local', key, complex_value)

        conf = gpc.config.load()
        self.assertTrue(complex_value == gpc.config.load()[key])
        val = yaml.safe_load(
            check_output('gpc config get {}'.format(key).split(' ')).decode())
        self.assertTrue(val == conf[key])

    def tearDown(self):
        os.chdir(self.origdir)
        shutil.move(self.temp_global_backup, gpc.config.GLOBAL_PATH)
        shutil.rmtree(self.tempdir)

if __name__ == '__main__':
    unittest.main()
