import boyle

from glob import glob
import os

w = Workflow()

# list_col_values could be written in two ways.
# Either something like this:

@out('col_values.json')
@inp('table.csv')
def list_col_values(colname):
    return Python(script=f'''
        import pandas
        import json
        table = pandas.read_csv('table_path')
        values = list(set(table['{colname}'].values))
        with open('col_values.json', 'w') as f:
            json.dump(values, f)
        ''')

# Or something like this (with a list_col_values in a separate module):

@out('col_values.json')
@inp('table.csv')
def list_col_values(colname):
    return Python(
        func='peanuts.list_col_values',
        args=('table.csv', colname),
        serialize='table.csv')


# Note, the functions above are not tied to this workflow in any way.
# They could be placed in a separate module, or even a general-purpose lib.

# The Workflow object is probably not much more than a glorified dict
# with keys to identify the targets/nodes of the graph.
# It would be glorified at least in the sense that it prevents
# accidental over-writing of definitions, which could cause some disorder.
# Each value in the workflow dict is either a definition
# or a list of definitions. The former will probably be the most common,
# but the latter is useful when we want to map an operation over a set
# of definitions (the boyle.each() function). Perhaps also as a sort of
# meta-definition similar to GNU make. In this example, "boyle make files"
# should spit out all the csv files.

w['files'] = [
    boyle.project_dir(path)
    for path in glob('path/to/csv_files/*.csv')
    ]

colnames = ['pid', 'nid']


# Two levels of "sub-workflows" defined here.
# The first sub-level is indexed by colname; the second by the w['files']

w['colvals.json'] = list_col_values()
with boyle.each(colnames) as colname:
    with boyle.each('files') as w['file.csv']:
        w['colvals.json'] = list_col_values('file.csv', colname)

    w['colvals_per_file'] = boyle.collect('colvals.json', axis='file.csv')
    w['all_colvals.json'] = boyle.json.concat('colvals_per_file')
    w['unique.json'] = boyle.json.unique('colvals.json')

    # boyle.json.unique could be a nice part of a "standard library"

# this is problematic; I would like to do "as w['file.csv']"
# to look like the above, but that "slot" is already taken,
# and this would redefine w['file.csv']
with boyle.each('files') ?:
    ...?

# or simply (but with inconsistent indentation):

from some_module import make_matrix

w['pid_vals.json'] = w['unique.json']['pid']
w['nid_vals.json'] = w['unique.json']['nid']
w['matrix.h5'] = make_matrix('file.csv', 'pid_vals.json', 'nid_vals.json')