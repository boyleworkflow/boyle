import unittest
import tempfile
import os
import boyle

test_path = './test-output'
template_path = './tests/templates'


class TestLog(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_save_load_user(self):
        with tempfile.TemporaryDirectory() as d:
            log_path = os.path.join(d, 'log.db')
            u = boyle.User(user_id='my user id', name='my user name')

            log = boyle.Log(log_path)
            log.save_user(u)
            log.close()

            log = boyle.Log(log_path)
            self.assertEqual(log.get_user(u.user_id), u)
            log.close()

if __name__ == '__main__':
    unittest.main()
