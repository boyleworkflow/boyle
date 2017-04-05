
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

w['pid_vals.json'] = w['unique.json']['pid']
w['nid_vals.json'] = w['unique.json']['nid']
w['matrix.h5'] = make_matrix('file.csv', 'pid_vals.json', 'nid_vals.json')