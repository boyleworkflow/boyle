import os
import json
import sqlite3
import functools
from os.path import abspath

from gpc import common

from gpc.common import unique_json, hexdigest

class DatabaseError(Exception): pass


class Database(object):
    """Class to interact with the file-backed SQLlite database."""
    def __init__(self, path):
        """Open a database"""
        super(Database, self).__init__()
        path = abspath(path)
        self._data_path = Database._data_path(path)
        self._transaction = []

        try:
            with open(Database._schema_path(path), 'r') as f:
                schema = f.read()

            self._conn = sqlite3.connect(':memory:')
            with self._conn as conn:
                conn.executescript(schema)

                for filename in os.listdir(self._data_path):
                    path = os.path.join(self._data_path, filename)
                    with open(path, 'r') as f:
                        sql = f.read()
                    conn.execute(sql)
        except Exception as e:
            msg = "Database at '{}' could not be opened".format(path)
            raise DatabaseError(msg) from e

        self._conn.set_trace_callback(self._handle_stmt)

    @staticmethod
    def _data_path(path):
        return abspath(os.path.join(path, 'data'))

    @staticmethod
    def _schema_path(path):
        return abspath(os.path.join(path, 'schema.sql'))


    def _handle_stmt(self, stmt):
        if stmt.strip() in ('BEGIN', 'ROLLBACK'):
            self._transaction = []
        elif stmt == 'COMMIT':
            for s in self._transaction:
                self._write_statement(s)
        else:
            self._transaction.append(stmt)

    def _write_statement(self, stmt):
        if stmt.lstrip().lower().startswith('insert'):
            digest = hexdigest(stmt)
            path = os.path.join(self._data_path, digest)
            if not os.path.exists(path):
                with open(path, 'w') as file:
                    file.write(stmt)
                    file.write('\n')

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
        os.makedirs(path)
        os.makedirs(Database._data_path(path))
        open(Database._schema_path(path), 'w').write(schema)


    def __enter__(self, *args, **kwargs):
        self._conn.__enter__(*args, **kwargs)
        return self

    def __exit__(self, *args, **kwargs):
        self._conn.__exit__(*args, **kwargs)
        
    def execute(self, sql, parameters=()):
        return self._conn.execute(sql, parameters)

    def executemany(self, sql, parameters=()):
        return self._conn.executemany(sql, parameters)
