import os
import csv
import json

class Database(object):
    def __init__(self, path):
        super(Database, self).__init__()
        self.db_path = path

    def create_table(self, name, columns, key, index=None):
        """Create a table. This creates a new directory in the database
        directory.

        Args:
            name (str): Name to give table.
            columns (list): List of column names.
            key (list): List of columns that constitute the key.
            index (str): Name of column to index. Default indexes on key.
        """
        os.makedirs(os.path.join(self.db_path, 'tables', name))
        os.makedirs(os.path.join(self.db_path, 'metadata'))
        mdfilepath = os.path.join(self.db_path, 'metadata', name)
        ## TODO support index
        json.dump({'columns': columns, 'key': key}, open(mdfilepath, 'w'))

    def get_id(self, table, **row):
        """Get id of a row."""
        ## no need to query file system if row contains the key
        pass
    
    def insert(self, table, **row):
        """Insert a row.

        Args:
            table (str): Name of table to insert into.
            row (hash): Map where keys are column names.
        """
        ## calculate id
        row_id = get_id(table, **row)
        ## write file
        path = os.path.join(seld.db_path, 'tables', table, row_id)
        with open(path, 'w') as csvfile:
            writer = csv.writer(csvfile)
            spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])

    def select(self, table, **row):
        """Select a list of rows.

        Args:
            table (str): Name of table to insert into.
            row (hash): Map where keys are column names.
        """
        pass

def main():
    db = Database('gpcdb')
    db.create_table('test', 1, 1)

if __name__ == '__main__':
    main()
