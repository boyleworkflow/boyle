import unittest
import tempfile
import os
import datetime
from itertools import product, combinations_with_replacement
import boyle

test_path = './test-output'
template_path = './tests/templates'


def opinion_array_to_dict(arr, results, users):
    opinions = {result: {} for result in results}
    for (result, user), opinion in zip(product(results, users), arr):
        opinions[result][user] = opinion
    return opinions

def generate_conflict_cases(users, results):

    run_user = boyle.User(user_id='run_uid', name='Runner')

    opinion_cases = (
        opinion_array_to_dict(arr, results, users)
        for arr in product(
            [True, False, None],
            repeat=len(results) * len(users)
            )
        )

    for i, opinions in enumerate(opinion_cases):
        calc = boyle.Calc(inputs=[], task=boyle.Shell(f'do_{i+1}'))
        runs = [
            boyle.Run(
                run_id=f'run-{i}-{result.digest}',
                calc=calc,
                results=[result],
                start_time=datetime.datetime.utcnow(),
                end_time=datetime.datetime.utcnow(),
                user=run_user
                )
            for result in results
            ]

        yield calc, runs, opinions

def get_candidates(user, opinions):

    def is_candidate(result):
        # If user trusts
        if opinions[result][user] == True:
            return True

        # Or noone distrusts
        if False not in set(opinions[result].values()):
            return True

        # Otherwise (user distrusts, or at least one other distrusts)
        return False

    results = opinions.keys()
    candidates = filter(is_candidate, results)
    return set(candidates)


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

        instr = boyle.File('something')
        num_users = 3
        num_results = 2

        users = [
            boyle.User(user_id='uid{i+1}', name='user{i+1}')
            for i in range(num_users)
            ]

        results = [
            boyle.Resource(instr=instr, digest=f'digest{i+1}')
            for i in range(num_results)
            ]

        conflict_cases = generate_conflict_cases(users, results)

        the_user = users[0]

        with tempfile.TemporaryDirectory() as tempdir:
            log_path = os.path.join(tempdir, 'log.db')
            log = boyle.Log(log_path)
            for calc, runs, opinions in conflict_cases:
                expected_results = get_candidates(the_user, opinions)
                for run in runs:
                    log.save_run(run)
                for result, user_opinions in opinions.items():
                    for user, opinion in user_opinions.items():
                        if opinion is not None:
                            log.set_trust(
                                calc.calc_id,
                                instr.instr_id,
                                result.digest,
                                the_user.user_id,
                                opinion
                                )
                if expected_results == set():
                    with self.assertRaises(boyle.NotFoundException):
                        log.get_trusted_result(calc, instr, the_user)
                elif len(expected_results) == 1:
                    the_result, = expected_results
                    self.assertEqual(
                        the_result,
                        log.get_trusted_result(calc, instr, the_user))
                else:
                    with self.assertRaises(boyle.ConflictException):
                        log.get_trusted_result(calc, instr, the_user)


if __name__ == '__main__':
    unittest.main()
