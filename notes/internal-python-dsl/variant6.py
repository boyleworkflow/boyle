from boyle import *

define({'files': Files('path/to/csv_files/*.csv')})

with Each('files', 'f'):

    define(
        {
            'pids': Value('pids'),
            'nids': Value('nids')
        },
        'f',
        Python('''
            import peanuts
            with open(inp.path) as f:
                pids = peanuts.list_col_values(f, 'pid')
                nids = peanuts.list_col_values(f, 'nid')
            '''))


define(
    {'unique_pids': Value()},
    collect('pids'),
    Python('out = set.union(*(set(l) for l in inp'))

define(
    {'unique_nids': Value()},
    collect('nids'),
    Python('out = set.union(*(set(l) for l in inp'))


with Each('files', 'f'):

    define(
        {'matrix': Value()},
        {
            'rows': 'unique_pids',
            'cols': 'unique_nids',
            'file': 'f'
        },
        Python('''
            import peanuts
            peanuts.make_matrix(inp['rows'], inp['cols'], inp['file'].path)
            '''))
