
g = Graph('graph.yml')

for k,l,m in itertools.product(range(200), range(5), ['a','b']):
    g.params['k'] = k
    g.params['l'] = l
    g.params['m'] = m
    g.run()
