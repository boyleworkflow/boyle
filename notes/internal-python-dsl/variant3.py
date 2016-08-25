from boyle import *

w = Workflow()

define([w.csv_files, LocalFiles('path/to/csv_files', '*.csv')])

with Each(w.csv_files) as file:
    define(
        [
            w.pids ~ Value(),
            w.nids ~ Value()
        ],
        file,
        Python("""
            with open(inp.path) as f:
                out[0] = peanuts.list_pids(f)
                out[1] = peanuts.list_nids(f)
            """))

define(
    w.unique_nids ~ Value(),
    collect(w.nids),
    Python('out = peanuts.unique(inp)'))

define(
    w.unique_nids ~ Value(),
    collect(w.nids),
    Python('out = peanuts.unique(inp)'))

# alternatively:
#
# define(
#     [
#         w.unique_nids ~ Value(),
#         w.unique_pids ~ Value()
#     ],
#     [collect(w.nids), collect(w.pids)],
#     Python('''
#         for i, item in enumerate(inp):
#             out[i] = peanuts.unique(item)
#         '''))

with Each(w.csv_files) as file:
    define(
        w.matrix ~ Value(),
        {
            'rows': w.unique_pids,
            'cols': w.unique_nids,
            'file': file
        },
        Python('''
            out = peanuts.make_matrix(
                inp["rows"], inp["cols"], inp["file"].path)
            '''))
