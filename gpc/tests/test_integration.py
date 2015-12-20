import unittest
from scripttest import TestFileEnvironment

class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.env = TestFileEnvironment('./test-output',
                                       template_path='./gpc/tests/templates')

    def test_make_target(self):
        self.env.writefile('gpc.yaml', frompath='simple.yaml')
        result = self.env.run('gpc', 'make' 'c')
        self.assertTrue('c' in [f.path for f in result.files_created])

if __name__ == '__main__':
    unittest.main()

