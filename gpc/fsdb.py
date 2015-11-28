import os
import json
from gpc import common

from gpc.common import unique_json, hexdigest

class DatabaseError(Exception): pass

class Database(object):
    def __init__(self, path):
        """Create or open a database.

        A new database is created if database directory is empty.

        Args:
            path (str): The database directory.

        Raises:
            FileNotFoundError: If database directory is not empty
                but not all expected files/subdirs exist.
        """
        super(Database, self).__init__()
        self._path = os.path.abspath(path)        
        self._tables_path = os.path.join(self._path, 'tables')
        self._metadata_path = os.path.join(self._path, 'metadata')

        if not os.path.exists(self._path):
            os.makedirs(self._path)

        if not os.listdir(path):
            os.makedirs(self._tables_path)
            os.makedirs(self._metadata_path)

        self._metadata = {}
        for table in self.tables():
            mdfilepath = os.path.join(self._metadata_path, table)
            self._metadata[table] = json.load(open(mdfilepath, 'r'))


    def tables(self):
        """Get a list of tables."""
        return os.listdir(self._tables_path)


    def create_table(self, name, columns, key, index=None):
        """Create a table.

        This creates a new directory in the database directory.

        If only one column constitues the key, the values in that
        column are used as keys. In this case, make sure your
        values for that column are strings, and suitable as file names.

        Args:
            name (str): Name to give table.
            columns (list): List of column names.
            key (list): List of columns that constitute the key.
            index (str): Name of column to index. Default indexes on key.

        Raises:
            FileExistsError: If table directory already exists.
            DatabaseError: If not key columns is subset of columns.
        """
        os.makedirs(os.path.join(self._tables_path, name))
        mdfilepath = os.path.join(self._metadata_path, name)
        ## TODO support index
        if not set(key) <= set(columns):
            raise DatabaseError('key columns must be a subset of columns')
        metadata = {'columns': columns, 'key': key}
        json.dump(metadata, open(mdfilepath, 'w'))
        self._metadata[name] = metadata


    def _get_key(self, table, **row):
        """Get key of a row.

        Args:
            row (dict): Whole or part of a row, at least including
                the column names and values in the table key.

        Raises:
            KeyError: If any of the table's key columns are not in row.
            TypeError: If the key was not a suitable filename.
        """
        ## no need to query file system if row contains the key
        md = self._metadata[table]
        key_parts = [row[col] for col in md['key']]
        if len(key_parts) == 1:
            key = key_parts[0]
        else:
            key = hexdigest(unique_json(key_parts))
        if not isinstance(key, str):
            raise TypeError('key is not a string', key)
        return key


    def _get_row_path(self, table, key):
        # TODO support index
        return os.path.join(self._tables_path, table, key)


    def insert(self, table, **row):
        """Insert a row.

        Args:
            table (str): Name of table to insert into.
            **row: Map where keys are column names.

        Raises:
            KeyError: If any of the columns are not specified.
        """
        md = self._metadata[table]
        row = {col: row[col] for col in md['columns']}

        ## calculate key
        row_key = self._get_key(table, **row)
        ## write file
        path = self._get_row_path(table, row_key)

        # Check that another value has not been written
        # under the same key before. This should only be possible
        # if the key column set is misspecified.
        # TODO: remove this check?
        if os.path.exists(path):
            previous = json.load(open(path, 'r'))
            assert previous == row
            return

        with open(path, 'w') as f:
            # Slightly roundabout way to do it, but provides
            # a check that all columns of the table are included.
            json.dump(row, f)


    def select(self, table, **criteria):
        """Select a list of rows.

        Args:
            table (str): Name of table to insert into.
            **criteria: Search criteria to match against.
                Keys are column names and values are values to match.
        """
        try:
            # If key can be created, we will find one or zero records.
            key = self._get_key(table, **criteria)
            path = self._get_row_path(table, key)
            if os.path.exists(path):
                return [json.load(open(path, 'r'))]
            else:
                return []
        except KeyError:
            records = []
            keys = os.listdir(os.path.join(self._tables_path, table))
            for key in keys:
                path = self._get_row_path(table, key)
                record = json.load(open(path, 'r'))
                if all(record[col] == val for col, val in criteria.items()):
                    records.append(record)
            return records


def main():
    db = Database('gpcdb')
    if not 'test' in db.tables():
        db.create_table('test', ['c','b','a'], ['b','a'])

    row1 = dict(a=123, b='foo', c={'foo': 'bar'})
    row2 = dict(a=123, b='bar', c={'foo': 'bar'})
    db.insert('test', **row1)
    db.insert('test', **row2)

    # This cannot be written after row2 because key would be
    # identical but row contents different
    row3 = dict(a=123, b='bar', c='baz')
    # db.insert('test', **row3) # raises AssertionError

    print(db.select('test', a=123, b='foo'))
    print(db.select('test', a=123, b='bar'))
    assert row1 == db.select('test', a=123, b='foo')[0]
    assert row2 == db.select('test', a=123, b='bar')[0]


    if not 'test2' in db.tables():
        db.create_table('test2', ['b','a'], ['a'])
    db.insert('test2', a='key1', b=123)
    db.insert('test2', a='key2', b=456)

    # Again, cannot write two rows with same key
    # db.insert('test2', a='key2', b=0) # raises AssertionError

if __name__ == '__main__':
    main()
