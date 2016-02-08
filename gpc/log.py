import os
from gpc import fsdb
from gpc import NotFoundException
import sqlite3
from collections import defaultdict
import logging
import pkg_resources

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SCHEMA_VERSION = 'v0.0.0'

class Log(object):
    def __init__(self, path, user):
        """
        Open a log.

        Args:
            path: The path of the log.
        """
        super(Log, self).__init__()
        path = os.path.abspath(path)
        self._db = fsdb.Database(path)

        self.user = user

        self._db.execute(
            'INSERT OR REPLACE INTO user(user_id, name) VALUES (?, ?)',
            (user['id'], user['name']))

    @staticmethod
    def create(path):
        """Create a log"""
        schema_path = pkg_resources.resource_filename(
            __name__, "resources/schema-{}.sql".format(SCHEMA_VERSION))
        with open(schema_path, 'r') as f:
            schema_script = f.read()

        fsdb.Database.create(path, schema_script)


    def write(self):
        self._db.write()


    def save_task(self, task_id, definition, sysstate):
        with self._db as db:
            db.execute(
                'INSERT OR IGNORE INTO task(task_id, definition, sysstate) '
                'VALUES (?, ?, ?)',
                (task_id, definition, sysstate))


    def save_calculation(self, calc_id, task_id, inputs):
        with self._db as db:
            db.execute(
                'INSERT OR IGNORE INTO calculation(calc_id, task_id) '
                'VALUES (?, ?)',
                (calc_id, task_id))

            db.executemany(
                'INSERT OR IGNORE INTO uses (calc_id, path, digest) '
                'VALUES (?, ?, ?)',
                [(calc_id, p, d) for p, d in inputs.items()])


    def save_composition(self, comp_id, calc_id, subcomp_ids):
        with self._db as db:
            db.execute(
                'INSERT OR IGNORE INTO composition(comp_id, calc_id) '
                'VALUES (?, ?)', (comp_id, calc_id))

            db.executemany(
                'INSERT OR IGNORE INTO subcomposition (comp_id, subcomp_id) '
                'VALUES (?, ?)',
                [(comp_id, subcomp_id) for subcomp_id in subcomp_ids])

    def save_request(self, path, comp_id, time, digest):
        """Note: only saves the first time a user requests"""

        with self._db as db:
            db.execute(
                'INSERT OR IGNORE INTO '
                'requested(path, digest, user_id, firsttime, comp_id) '
                'VALUES (?, ?, ?, ?, ?)',
                (path, digest, self.user['id'], time, comp_id))


    def save_run(self, run_id, info, time, calc_id, digests):

        with self._db as db:
            db.execute(
                'INSERT INTO run(run_id, user_id, info, time, calc_id) '
                'VALUES (?, ?, ?, ?, ?)',
                (run_id, self.user['id'], info, time, calc_id))

            db.executemany(
                'INSERT INTO created(run_id, path, digest) VALUES (?, ?, ?)',
                [(run_id, p, d) for p, d in digests.items()])


    def get_digest(self, calc_id, path):
        """Try to find the digest of a calculation output.

        Args:
            calc_id (str): The id of the calculation in question.
            path (str): The relative path of the output.

        Returns:
            None: If the output cannot be found.
            str: A file digest, if exactly one is found.

        Raises:
            NotFoundException: If there is no candidate value.
            ConflictException: If there are more than one candidate values.
        """

        candidates = self._db.execute(
            'SELECT DISTINCT digest, trust.user_id, correct FROM created '
            'INNER JOIN run USING (run_id) '
            'LEFT OUTER JOIN trust USING (calc_id, digest, path) '
            'WHERE (path = ? AND calc_id = ?)',
            (path, calc_id))

        opinions = defaultdict(lambda: defaultdict(list))
        for digest, user, correct in candidates:
            opinions[digest][correct].append(user)

        # If there are any digests which the current user does not
        # explicitly think anything about, and for which other users
        # have mixed opinions, there is a conflict:

        logger.debug((
            'get_digest:\n'
            '\tcalc_id: %s\n'
            '\tpath: %s') % (calc_id, path))
        logger.debug('opinions: {}'.format(dict(opinions)))

        for digest, users in opinions.items():
            if (
                self.user['id'] not in users[True] + users[False] and
                len(users[True]) > 0 and
                len(users[False]) > 0):
                raise ConflictException(calc_id, path)


        # Remove all digests which noone trusts and at least one distrusts
        opinions = {
            digest: users for digest, users in opinions.items()
            if not (len(users[True]) == 0 and len(users[False]) > 0)}

        # Remove all digests the current user distrusts
        opinions = {
            digest: users for digest, users in opinions.items()
            if not self.user['id'] in users[False]}

        # If there is no digest left, nothing is found.
        # If there is exactly one left, it can be used.
        # If there is more than one left, there is a conflict.

        if len(opinions) == 0:
            raise NotFoundException()
        elif len(opinions) == 1:
            digest, = opinions.keys()
            return digest
        else:
            raise ConflictException(calc_id, path)

    def get_conflict(self, calc_id, path):
        """Get info about a conflict over an output of a calculation.

        Args:
            calc_id (str): The id of the calculation.
            path (str): The relative path of the output.

        Returns:
            A list of lists, containing all the Run objects that are
            involved in the conflict. All the runs in each list are
            in agreement with each other.
        """
        raise NotImplementedError()


    def get_provenance(self, digest):
        return ['Placeholder provenance']
        raise NotImplementedError()
