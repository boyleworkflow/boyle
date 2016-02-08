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
        conf = gpc.config.load_settings()

        # From global file
        self.assertTrue(conf == dict(user=dict(name=NAME, id=ID)))


        # No local file exists
        self.assertTrue(not os.path.exists(gpc.config.LOCAL_PATH))

        # Create local
        call(['gpc', 'config', 'set', '--local', 'user.name', NAME + 'local'])
        self.assertTrue(os.path.exists(gpc.config.LOCAL_PATH))

        # Check that local file overrides
        conf = gpc.config.load_settings()
        self.assertTrue(conf == dict(user=dict(name=NAME + 'local', id=ID)))

    def test_nested(self):
        # Write nested
        call('gpc config set a.b.c.d 123'.split(' '))

        # Read with API
        conf = gpc.config.load_settings()
        self.assertTrue(conf['a'] == {'b':{'c':{'d': '123'}}})

        # Read with CLI
        value = check_output('gpc config get a.b.c.d'.split(' ')).decode()
        self.assertTrue(value.strip() == '123')

    def test_complex(self):
        complex_value = dict(abc=1, cde=["str", 123])
        loc = 'a.b.c.d'
        gpc.config.set_config('local', loc, complex_value)

        conf = gpc.config.load_settings()
        self.assertTrue(complex_value == gpc.config.get_location(conf, loc))
        val = yaml.load(
            check_output('gpc config get a.b'.split(' ')).decode())
        self.assertTrue(val == conf['a']['b'])

    def tearDown(self):
        os.chdir(self.origdir)
        shutil.move(self.temp_global_backup, gpc.config.GLOBAL_PATH)
        shutil.rmtree(self.tempdir)

if __name__ == '__main__':
    unittest.main()
