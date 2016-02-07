import os
import json
import sqlite3
import functools
import itertools
from os.path import abspath
import logging
from gpc import hexdigest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DatabaseError(Exception): pass


class Database(object):
    """Class to interact with the file-backed SQLlite database."""
    def __init__(self, path):
        """Open a database"""
        super(Database, self).__init__()
        path = abspath(path)
        self._schema_dir = Database._get_schema_dir(path)
        self._data_dir = Database._get_data_dir(path)

        def statements():
            dirs = (
                self._schema_dir,
                self._data_dir)

            for d in dirs:
                for file in os.listdir(d):
                    stmt_path = os.path.join(d, file)
                    with open(stmt_path, 'r') as f:
                        sql = f.read()
                    yield sql

        try:
            self._conn = sqlite3.connect(':memory:')
            with self._conn as conn:
                for stmt in statements():
                    conn.execute(stmt)
        except Exception as e:
            msg = "Database at '{}' could not be opened".format(path)
            raise DatabaseError(msg) from e


    @staticmethod
    def _get_data_dir(path):
        return abspath(os.path.join(path, 'data'))

    @staticmethod
    def _get_schema_dir(path):
        return abspath(os.path.join(path, 'schema'))

    def write(self):
        statements = self._conn.iterdump()
        assert next(statements) == 'BEGIN TRANSACTION;'
        for stmt in statements:
            if stmt == 'COMMIT;':
                break
            if stmt.lower().startswith('create table'):
                target_dir = self._schema_dir
            else:
                target_dir = self._data_dir
            digest = hexdigest(stmt)
            path = os.path.join(target_dir, digest)
            if not os.path.exists(path):
                with open(path, 'w') as file:
                    file.write(stmt)
                    file.write('\n')
        try:
            next_stmt = next(statements)
            raise DatabaseError('unexpected statement {}'.format(next_stmt))
        except StopIteration:
            pass

    def __enter__(self):
        return self._conn.__enter__()

    def __exit__(self, *args, **kwargs):
        self._conn.__exit__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._conn.executemany(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self._conn.executescript(*args, **kwargs)

    @classmethod
    def create(cls, path):
        """
        Create a new database

        Raises:
            DatabaseError: If path exists.
        """

        path = abspath(path)
        if os.path.exists(path):
            raise DatabaseError('Path must not exist when creating database!')
        os.makedirs(Database._get_schema_dir(path))
        os.makedirs(Database._get_data_dir(path))
