from boyle import define, File, Shell

a = define(
    name='t1',
    out=File('a'),
    do=Shell('echo hello > {out}'))

b = define(
    name='t2',
    out=File('b'),
    do=Shell('echo world > {out}'))

c = define(
    name='t3',
    out=File('jonatan.jpg'),
    inp=[a, b],
    do=Shell('cat {inp[0]} {inp[1]} > {out}'))

