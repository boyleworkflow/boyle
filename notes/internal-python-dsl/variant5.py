from boyle import *

files = define(Files('path/to/csv_files/*.csv'))

with Each(files) as f:

    pids, nids = define(
        [
            Value('pids'),
            Value('nids')
        ]
        f,
        Python('''
            import peanuts
            with open(inp.path) as f:
                pids = peanuts.list_col_values(f, 'pid')
                nids = peanuts.list_col_values(f, 'nid')
            '''))


unique_pids = define(
    Value('out'),
    collect(pids),
    Python('out = set.union(*(set(l) for l in inp'))

unique_nids = define(
    Value('out'),
    collect(nids),
    Python('out = set.union(*(set(l) for l in inp'))


with Each(files) as f:

    matrix = define(
        Value(),
        {
            'rows': unique_pids,
            'cols': unique_nids,
            'file': f
        },
        Python('''
            import peanuts
            peanuts.make_matrix(inp['rows'], inp['cols'], inp['file'].path)
            '''))
