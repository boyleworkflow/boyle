# No Workflow object really needed? The workflow is implicitly
# defined through the interconnection of the definitions.
# The Promise type represents a promise for later creation of a result,
# roughly saying "give me the following input, and I'll give you some output"
# in return.

csv_dir = Promise(None, LocalDir('path/to/csv_dir'))

csv_files = Promise(
    csv_dir,
    lambda inp: inp.list_files('*.csv'))

with Each(csv_files) as f:
    pids = Promise(f, Python('out = peanuts.list_pids(inp.path)'))
    nids = Promise(f, Python('out = peanuts.list_nids(inp.path)'))


# This is not tied to the Promise in any way. It's just something callable.
unique_transformer = Python('out = peanuts.unique(inp)')

lists_of_pids = Collect(pids)
unique_pids = Promise(lists_of_pids, unique_transformer)

# or, inlined
unique_nids = Promise(Collect(nids), unique_transformer)

with Each(csv_files) as f:
    matrix = Promise(
        {
            'rows': unique_pids,
            'cols': unique_nids,
            'file': f
        },
        Python('''
            out = peanuts.make_matrix(inp['rows'], inp['cols'], inp['file'])
            ''')
        )

# The handlers/task functions should be composable, e.g. in sequences
# Then, a terrible little tool with return value and 
# some side effect could be used as follows:

p = Promise(
    some_inputs,
    [
        Python('out = function_with_side_effect()'),
        LocalFile('the/side_effect/file.txt')
    ]
    )

# This should return a Promise with two items, which could perhaps
# be accessed like p[0] and p[1]. Or, if the Promise is iterable one could
# even write
# value, file = Promise(..., [..., ...])