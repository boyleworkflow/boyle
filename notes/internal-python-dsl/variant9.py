# VARIANT 9

# a.k.a "the implicit variant"
# a.k.a "stop naming files"


import boyle

from glob import glob
import os

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


files = [boyle.project_dir(path) for path in glob('path/to/csv_files/*.csv')]
colnames = ['pid', 'nid']


with boyle.each(colnames) as colname:
    with boyle.each(files) as file:
        colvals = list_col_values(file, colname)

    all_colvals = boyle.json.concat(colvals)
    unique_colvals = boyle.json.unique(all_colvals)

# The above should be equivalent to something like

# colname, file = boyle.product(colnames, files)
# colvals = list_col_values(file, colname)
# all_colvals = boyle.json.concat(colvals)
# unique_colvals = boyle.json.unique(all_colvals)

from some_module import make_matrix
pids, nids = unique_colvals['pid'], unique_colvals['nid']
with boyle.each(files) as file:
    matrix = make_matrix(file, pids, nids)


#####################################

@out('some_directory')
@inp('inputs/{}/arne.tif')
def collect():
    pass

@out('out/1', 'out/2')
@inp('inp/1', 'inp/2')
def something(param1):
    return [
        Shell(f'./something inp/1 inp/2 -p {param1} -o out/1'),
        Shell('./something_else inp/1 inp/2 -o out/2'),
        ]

# is just shorthand for

def something(inp, param1):
    internal_inp_names = ('inp/1', 'inp/2')
    inp_rename = {old: new for old, new in zip(inp, internal_inp_names)}

    ops = [
        Rename(inp_rename),
        Shell(f'./something inp/1 inp/2 -p {param1} -o out/1'),
        Shell('./something_else inp/1 inp/2 -o out/2')
        ]

    out_names = ('out/1', 'out/2')

    return Task(out=out_names, inp=inp, ops=ops).outputs


# Another example:
def build_dict(inp, key):
    outname = 'the_dict.json'
    internal_inp_names = [f'inputs/{i}' for i in range(len(inp))]
    inp_rename = {old: new for old, new in zip(inp, internal_inp_names)}
    ops = [
        Rename(inp),
        BuildJSONDict(internal_inp_names, outname)
    ]
