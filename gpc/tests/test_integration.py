import unittest
from scripttest import TestFileEnvironment
from subprocess import call

test_path = './test-output'
template_path = './gpc/tests/templates'

class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.env = TestFileEnvironment(test_path, template_path=template_path)

    def test_make_target_cached(self):
        call(['cp', '-r', template_path+'/log', test_path])
        call(['cp', '-r', template_path+'/storage', test_path])
        call(['cp', '-r', template_path+'/.gpc', test_path])
        self.env.writefile('gpc.yaml', frompath='simple.yaml')
        result = self.env.run('gpc', 'make', 'c', expect_stderr=True)
        created_filenames = result.files_created.keys()
        self.assertTrue('c' in created_filenames)
        self.assertTrue(len(created_filenames) == 1)

if __name__ == '__main__':
    unittest.main()
