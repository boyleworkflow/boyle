import unittest
from scripttest import TestFileEnvironment
from subprocess import call, check_output

test_path = './test-output'
template_path = './tests/templates'

class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.env = TestFileEnvironment(test_path, template_path=template_path)

    def test_init(self):
        result = self.env.run('gpc', 'init', expect_stderr=True)
        created_filenames = result.files_created.keys()
        self.assertTrue('log' in created_filenames)
        self.assertTrue('log/data' in created_filenames)
        self.assertTrue('log/schema.sql' in created_filenames)
        self.assertTrue('storage' in created_filenames)

    def test_make_target(self):
        self.env.run('gpc', 'init', expect_stderr=True)
        self.env.writefile('gpc.yaml', frompath='simple.yaml')
        result = self.env.run('gpc', 'make', 'c', expect_stderr=True)
        created_filenames = list(result.files_created.keys())
        self.assertTrue('c' in created_filenames)
        created_filenames.remove('c')
        self.assertTrue(
            any([s.startswith('storage/') for s in created_filenames]))
        self.assertTrue(
            any([s.startswith('log/data/') for s in created_filenames]))

    def test_make_target_cached(self):
        call(['cp', '-r', template_path+'/.gpc', test_path])
        call(['cp', '-r', template_path+'/log', test_path])
        call(['cp', '-r', template_path+'/storage', test_path])
        self.env.writefile('gpc.yaml', frompath='simple.yaml')
        result = self.env.run('gpc', 'make', 'c', expect_stderr=True)
        created_filenames = result.files_created.keys()
        self.assertTrue('c' in created_filenames)
        self.assertTrue(len(created_filenames) == 1)

if __name__ == '__main__':
    unittest.main()
