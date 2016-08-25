from boyle import *

csv_files = define(LocalFiles('csv_files', '*.csv'))

with Each(csv_files) as file:

    pids, nids = define(
        [Value(), Value()],
        file,
        Python("""
            with open(inp.path) as f:
                out[0] = peanuts.list_pids(f)
                out[1] = peanuts.list_nids(f)
            """))

lists_of_pids = collect(pids)

lists_of_nids = collect(nids)

unique_nids = define(
    Value(),
    lists_of_nids,
    Python('out = peanuts.unique(inp)'))

unique_pids = define(
    Value(),
    lists_of_pids,
    Python('out = peanuts.unique(inp)'))


with Each(csv_files) as file:
    matrix = define(
        Value(),
        {
            'rows': unique_pids,
            'cols': unique_nids,
            'file': file
        },
        Python('''
            out = peanuts.make_matrix(
                inp["rows"], inp["cols"], inp["file"].path)
            '''))
