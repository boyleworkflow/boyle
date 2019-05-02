import attr

import boyle

@attr.s(auto_attribs=True, frozen=True, cmp=False)
class Op:
    cmd: int
    out_locs = ['cmd']

    def __hash__(self):
        return hash(self.op_id)

    @property
    def op_id(self):
        return f'op_{self.cmd}'

    def run(self, inputs, storage):
        digest = f'{self.cmd}*2'
        digests = {'cmd': digest}
        storage.contents[digest] = self.cmd * 2
        return digests


@attr.s(auto_attribs=True)
class Storage:
    contents: dict

    def can_restore(self, digest: str) -> bool:
        return digest in self.contents


storage = Storage({})

log = boyle.Log('log.sqlite')

op2 = Op(2)

def make_comp(n):
    return boyle.Comp(op=Op(n), inputs={}, out_loc='cmd')

c2 = make_comp(2)
print(c2)

results = boyle.make([c2], log, storage)

print(results)

print(storage.contents[results[c2]])