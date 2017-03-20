import unittest
import tempfile
import os
import datetime
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

    def test_conflict(self):

        d = boyle.Def(
                instr=boyle.File('path/to/file'),
                parents=[],
                task=boyle.Shell('do_something')
            )

        calc = boyle.Calc(inputs=[], task=d.task)

        digest1 = 'digest1'
        digest2 = 'digest2'

        result1 = boyle.Resource(instr=d.instr, digest=digest1)
        result2 = boyle.Resource(instr=d.instr, digest=digest2)

        user1 = boyle.User(user_id='uid1', name='User1')
        user2 = boyle.User(user_id='uid2', name='User2')

        run1a = boyle.Run(
            run_id='run id 1',
            calc=calc,
            results=[result1],
            start_time=datetime.datetime.utcnow(),
            end_time=datetime.datetime.utcnow(),
            user=user1
            )

        run1b = boyle.Run(
            run_id='run id 2',
            calc=calc,
            results=[result1],
            start_time=datetime.datetime.utcnow(),
            end_time=datetime.datetime.utcnow(),
            user=user1
            )

        run1c = boyle.Run(
            run_id='run id 3',
            calc=calc,
            results=[result2],
            start_time=datetime.datetime.utcnow(),
            end_time=datetime.datetime.utcnow(),
            user=user1
            )

        with tempfile.TemporaryDirectory() as tempdir:
            log_path = os.path.join(tempdir, 'log.db')
            log = boyle.Log(log_path)

            log.save_run(run1a)
            res = log.get_trusted_result(calc, d.instr, user1)
            self.assertEqual(res, result1)

            log.save_run(run1b)
            res = log.get_trusted_result(calc, d.instr, user1)
            self.assertEqual(res, result1)

            log.save_run(run1c)
            with self.assertRaises(boyle.ConflictException):
                log.get_trusted_result(calc, d.instr, user1)

            try:
                log.get_trusted_result(calc, d.instr, user1)
            except boyle.ConflictException as e:
                candidates = e.resources

            self.assertEqual(candidates, (result1, result2))






if __name__ == '__main__':
    unittest.main()
