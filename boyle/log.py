import os
import sqlite3
import logging
import pkg_resources
import boyle
import attr

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 'v0.0.0'

class Log:

    @staticmethod
    def create(path):
        """
        Create a new Log database.

        Args:
            path (str): Where to create the database.
        """
        schema_path = pkg_resources.resource_filename(
            __name__, "resources/schema-{}.sql".format(SCHEMA_VERSION))

        with open(schema_path, 'r') as f:
            schema_script = f.read()

        conn = sqlite3.connect(path)

        with conn:
            conn.executescript(schema_script)

        conn.close()

    def __init__(self, path):
        if not os.path.exists(path):
            Log.create(path)
        self.conn = sqlite3.connect(path)

    def close(self):
        self.conn.close()

    def save_calc(self, calc):
        with self.conn:
            self.conn.execute(
                'INSERT OR IGNORE INTO calc(calc_id, op) '
                'VALUES (?, ?)',
                (calc.calc_id, calc.op))

            self.conn.executemany(
                'INSERT OR IGNORE INTO input (calc_id, loc, digest) '
                'VALUES (?, ?, ?)',
                [
                    (calc.calc_id, inp.loc, inp.digest)
                    for inp in calc.inputs
                ])

    def _comp_exists(self, c):
        self.conn.execute(
            'SELECT COUNT(comp_id) FROM def WHERE (comp_id = ?)',
            (d.comp_id,))

        return cur.fetchone() != None

    def save_comp(self, comp):
        for p in comp.parents:
            if not _comp_exists(p):
                raise ValueError(
                    f'parent {p} of composition {comp} must be saved first')

        self.save_calc(comp.calc)

        with self.conn:
            self.conn.execute(
                'INSERT OR IGNORE INTO comp(comp_id, calc_id, loc) '
                'VALUES (?, ?, ?)',
                (d.comp_id, d.calc.calc_id, d.loc))

            self.conn.executemany(
                'INSERT OR IGNORE INTO parent(comp_id, parent_id) '
                'VALUES (?, ?)',
                [(d.comp_id, p.comp_id) for p in d.parents])

    def save_user(self, user):
        with self.conn:
            self.conn.execute(
                'INSERT OR REPLACE INTO user(user_id, name) VALUES(?, ?)',
                (user.user_id, user.name))

    def get_user(self, user_id):
        q = self.conn.execute(
            'SELECT name FROM user WHERE (user_id = ?)',
            (user_id,)
            )
        name, = q.fetchone()
        return boyle.User(user_id=user_id, name=name)

    def save_run(self, run):
        logger.debug(f'Saving run {run}')
        self.save_calc(run.calc)
        self.save_user(run.user)

        with self.conn:
            self.conn.execute(
                'INSERT INTO run '
                '(run_id, calc_id, user_id, start_time, end_time) '
                'VALUES (?, ?, ?, ?, ?)',
                (
                    run.run_id,
                    run.calc.calc_id,
                    run.user.user_id,
                    run.start_time,
                    run.end_time
                    )
                )

            self.conn.executemany(
                'INSERT OR IGNORE INTO result (run_id, loc, digest) '
                'VALUES (?, ?, ?)',
                [
                    (run.run_id, resource.loc, resource.digest)
                    for resource in run.results
                ])

    def set_trust(self, calc_id, loc, digest, user_id, opinion):
        with self.conn:
            self.conn.execute(
                'INSERT OR REPLACE INTO trust '
                '(calc_id, loc, digest, user_id, opinion) '
                'VALUES (?, ?, ?, ?, ?) ',
                (calc_id, loc, digest, user_id, opinion)
                )


    def _get_opinions_by_resource(self, calc, loc):
        query = self.conn.execute(
            'SELECT DISTINCT digest, trust.user_id, opinion FROM result '
            'INNER JOIN run USING (run_id) '
            'LEFT OUTER JOIN trust USING (calc_id, loc, digest) '
            'WHERE (loc = ? AND calc_id = ?)',
            (loc, calc.calc_id))

        opinions = {}
        logger.debug('Getting opinions')
        for digest, user_id, opinion in query:
            logger.debug(
                f'digest: {digest}, user_id: {user_id}, opinion: {opinion}')
            resource = boyle.Resource(loc=loc, digest=digest)
            if not resource in opinions:
                opinions[resource] = {}
            if user_id:
                opinions[resource][user_id] = opinion

        return opinions

    def get_result(self, calc, loc, user):
        opinions_by_resource = self._get_opinions_by_resource(calc, loc)

        def is_candidate(resource):
            opinions = opinions_by_resource[resource]

            # A result is a candidate if there are no explicit opinions
            if not opinions:
                return True

            # If the user has an explicit opinion, that trumps everything else
            if user.user_id in opinions:
                return opinions[user.user_id]

            # Otherwise, let's see what others think
            # (if we come here, there must be at least one other opinion)
            those_in_favor = {
                user_id for user_id, opinion in opinions.items() if opinion
                }

            return True if those_in_favor else False

        candidates = tuple(r for r in opinions_by_resource if is_candidate(r))

        # If there is no digest left, nothing is found.
        # If there is exactly one left, it can be used.
        # If there is more than one left, there is a conflict.

        if not candidates:
            raise boyle.NotFoundException()
        elif len(candidates) == 1:
            match, = candidates
            return match
        else:
            raise boyle.ConflictException(candidates)


    def get_calculation(self, comp, user):

        def get_result(parent):
            calc = self.get_calculation(parent, user)
            return self.get_result(calc, parent.loc, user)

        inputs = tuple(get_result(p) for p in comp.parents)
        return boyle.Calc(inputs=inputs, op=comp.op)
