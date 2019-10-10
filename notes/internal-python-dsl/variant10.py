import boyleworkflow.api as api

from other_module import list_col_values, make_matrix

files = ...
col_names = ['pid', 'nid']

def get_unique_col_names(col_name):

    col_val_lists = [
        api.python(list_col_values, f, col_name)
        for f in files
        ]

    unique_vals = api.python(set.union, *col_val_lists)

    return unique_vals


pids = get_unique_col_names('pid')
nids = get_unique_col_names('nid')

matrices = {
    file: make_matrix(file, pids, nids)
}


####


TABLE_NAMES = ['ef_lus_allcrops', 'apro_cpshr']
COUNTRIES = [...]

def transform_and_load(name):
    file = (
        Shell(f'eust export --format csv {name} data.csv')
        .output('data.csv')
        )

    with file:
        transformed = (
            Shell(f'transform_data -o transformed.csv {file}')
            .output('transformed.csv')
            )

    Task(
        Shell(f'transform_data -o transformed.csv {file}')
        inp=[file]
        )



    return (
        Python(pd.read_csv, transformed, index_cols=['geo', 'time', 'crops'])
        .value
        )

tables = {name: dump_table(name) for name in TABLE_NAMES}

