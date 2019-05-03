import attr

import boyle

storage = boyle.Storage('storage_dir')
log = boyle.Log('log.sqlite')

a = boyle.shell('echo hello > a', inputs={}, out='a')
# b = boyle.shell('echo hello > a')

# print(a)
#
b = boyle.shell('echo world > b', inputs={}, out='b')

c = boyle.shell('cat x y > c', inputs={'x': a, 'y': b}, out='c')

print(c)

# results = boyle.make([a], log, storage)
# results = boyle.make([b], log, storage)
results = boyle.make([c], log, storage)

for comp, digest in results.items():
    print(comp.comp_id, digest)
