import unittest
import pkg_resources
import os
import tempfile
import shutil
import logging
import boyle

logging.basicConfig(level=logging.DEBUG)

class TestBoyle(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def test_file_output(self):
        filename = 'foo.txt'
        file_contents = 'bar'

        foo = boyle.define(
            out=boyle.File(filename),
            do=boyle.Shell('echo {} > {}'.format(file_contents, filename)))

        boyle.deliver(foo, self.tempdir)

        with open(os.path.join(self.tempdir, filename)) as f:
            self.assertTrue(f.read().strip() == file_contents)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

if __name__ == '__main__':
    unittest.main()
