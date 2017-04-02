from boyle import *

def get_col_values(colname):
    return Task(
        out='values.json',
        inp='table.csv',
        op=Python(f'''
            import peanuts
            vals = peanuts.list_col_values({colname}, 'table.csv')
            ''',
            save={'values.json': 'vals'}
            )
        )

get_unique_vals = Task('unique.json', 'values.json',
    Python(
        'unique = set(values)',
        save={'unique.json': 'unique'},
        load={'values': 'values.json'}
        )
    )

# boyle.json.concat_lists could exist in a "standard library"
# and would do something like this:
concat_json_lists = Task(
    'concatenated.json',
    'individual/{i}.json',
    Python('''
        import json
        import itertools
        d = 'individual'
        files = os.listdir(d)
        paths = (os.path.join(d, f) for f in files)
        def read_one(p):
            with open(p, 'r') as file:
                return json.load(file)
        big_list = list(itertools.chain(*map(read_one, paths)))
        ''',
        save={'concatenated.json': 'big_list'}
        )
    )

files = boyle.import_files('path/to/csv_files/*.csv')

# Could also have done
# files = Task(out='path/to/csv_files/*.csv')()

with Each(files) as file:
    pids = get_col_values('pid')(file)
    nids = get_col_values('nid')(file)


unique_pids = get_unique_vals(concat_json_lists(List(pids)))
unique_nids = get_unique_vals(concat_json_lists(List(nids)))

# Or, to avoid caching of the intermediate "concat_json_lists":
# boyle.compose(get_unique_vals, concat_json_lists)

with Each(files) as f:

    make_matrix = Task(
        'matrix.h5',
        ('rows.json', 'cols.json', 'data.csv'),
        PyFunc('tasks.make_matrix')
        )

    matrix = make_matrix(f)



# in a separate file py_tasks.py:

import util
import pandas as pd
import peanuts

def make_matrix():
    cols = util.load('cols.json')
    rows = util.load('rows.json')
    table = pd.read_csv('data.csv')
    matrix = peanuts.make_matrix(rows, cols, table)
    util.save(matrix, 'matrix.h5')
