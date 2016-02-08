import os
import json
import sqlite3
import functools
import itertools
import shutil
from os.path import abspath
import logging
from gpc import hexdigest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DatabaseError(Exception): pass


class Database(object):
    """Class to interact with the plain text-backed sqlite database."""
    def __init__(self, path):
        """Open a database"""
        super(Database, self).__init__()
        path = abspath(path)

        schema_script = Database._get_schema_script(path)
        self._data_dir = Database._get_data_dir(path)

        def data_statements():
            for file in os.listdir(self._data_dir):
                stmt_path = os.path.join(self._data_dir, file)
                with open(stmt_path, 'r') as f:
                    sql = f.read()
                yield sql

        self._conn = sqlite3.connect(':memory:')
        with self._conn as conn:
            conn.executescript(schema_script)

            for stmt in data_statements():
                conn.execute(stmt)


    @staticmethod
    def _get_data_dir(path):
        return abspath(os.path.join(path, 'data'))

    @staticmethod
    def _get_schema_script(path):
        with open(Database._get_schema_path(path), 'r') as f:
            return f.read()


    @staticmethod
    def _get_schema_path(path):
        return abspath(os.path.join(path, 'schema.sql'))

    def write(self):
        statements = self._conn.iterdump()
        def should_be_saved(stmt):
            return stmt.startswith('INSERT')

        for stmt in filter(should_be_saved, statements):
            digest = hexdigest(stmt)
            path = os.path.join(self._data_dir, digest)
            if not os.path.exists(path):
                with open(path, 'w') as file:
                    file.write(stmt)
                    file.write('\n')


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
    def create(cls, path, schema):
        """
        Create a new database

        Raises:
            DatabaseError: If path exists.
        """

        path = abspath(path)
        if os.path.exists(path):
            raise DatabaseError('Path must not exist when creating database!')

        os.makedirs(Database._get_data_dir(path))
        with open(Database._get_schema_path(path), 'w') as f:
            f.write(schema)

        # Test it
        try:
            db = Database(path)
        except Exception as e:
            shutil.rmtree(path)
            raise e
