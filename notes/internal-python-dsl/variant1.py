from boyle import *

w = Workflow()

w.define(
    out='csv_files',
    inp=LocalDir('csv_files'),
    Python("out = boyle.list_files(inp, '*.csv')"))


w.define(
    out='csv_files',
    inp=LocalDir('path/to/csvdir'),
    recipe=Python("out = boyle.list_files(inp, '*.csv')"))

with w.each('csv_files', 'file'):

    w.define('pids', 'file',
        Python("out = peanuts.list_pids(inp.path)"))

    w.define('nids', 'file',
        Python('out = peanuts.list_nids(inp.path)'))

w.define(
    out='lists_of_nids',
    inp='nids',
    Collect('nids', 'list_of_nids'))

w.define(
    out='lists_of_pids',
    inp='pids',
    Collect('pids', 'list_of_pids'))

w.define(
    out='unique_nids',
    inp='lists_of_nids',
    Python('out = peanuts.unique(lists_of_nids)'))

w.define(
    out='unique_pids',
    inp='lists_of_pids',
    Python('out = peanuts.unique(lists_of_pids)'))

with w.each('csv_files', file):
    w.define(
        'matrix',
        {
            'rows': 'unique_pids',
            'cols': 'unique_nids',
            'file': 'file'
        },
        Python('''
            out = peanuts.make_matrix(
                inp["rows"], inp["cols"], inp["file"].path)
            '''))
