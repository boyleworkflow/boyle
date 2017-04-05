# VARIANT 9

# a.k.a "the implicit variant"
# a.k.a "stop naming files"


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


files = [boyle.project_dir(path) for path in glob('path/to/csv_files/*.csv')]
colnames = ['pid', 'nid']

with boyle.each(colnames) as colname:
    with boyle.each(files) as file:
        colvals = list_col_values(file, colname)

    all_colvals = boyle.json.concat(boyle.collect(colvals))
    unique_colvals = boyle.json.unique(all_colvals)

# The above should be equivalent to something like

# colname, file = boyle.product(colnames, files)
# colvals = list_col_values(file, colname)
# all_colvals = boyle.json.concat(boyle.collect(colvals))
# unique_colvals = boyle.json.unique(all_colvals)

from some_module import make_matrix
pids, nids = unique_colvals['pid'], unique_colvals['nid']
with boyle.each(files) as file:
    matrix = make_matrix(file, pids, nids)
