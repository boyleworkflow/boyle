import functools
import json
import hashlib
import attr
from attr.validators import instance_of

digest_func = hashlib.sha1

def digest_str(s):
    return digest_func(s.encode('utf-8')).hexdigest()

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)

def digest_file(path):
    with open(path, 'rb') as f:
        return boyle.digest_func(f.read()).hexdigest()


def id_property(func):

    @property
    @functools.wraps(func)
    def id_func(self):
        id_obj = func(self)
        try:
            json = unique_json(id_obj)
        except TypeError as e:
            msg = f'The id_obj of {self} is not JSON serializable: {id_obj}'
            raise TypeError(msg) from e
        id_obj = {
            'type': type(self).__qualname__,
            'id_obj': id_obj
            }
        return digest_str(json)

    return id_func

class NotFoundException(Exception): pass

@attr.s
class ConflictException(Exception):
    resources = attr.ib(validator=instance_of(tuple))

@attr.s
class Rule:
    out = attr.ib(validator=instance_of(tuple))
    inp = attr.ib(validator=instance_of(tuple))
    op = attr.ib()

    @id_property
    def rule_id(self):
        return {
            'out': self.out,
            'inp': self.inp,
            'op': self.op.op_id
        }

@attr.s
class Comp:
    parents = attr.ib()
    rule = attr.ib()
    loc = attr.ib()

    def __attrs_post_init__(self):
        self.parents = tuple(self.parents)

    def __lt__(self, other_comp):
        my_ancestors = _get_ancestors([self])
        return other_comp not in my_ancestors

    @id_property
    def comp_id(self):
        return {
            'loc': self.loc,
            'parents': [p.comp_id for p in self.parents],
            'rule': self.rule.rule_id
        }

    @staticmethod
    def _get_ancestors(comps):
        comps = set()
        new_comps = set(comps)
        while new_comps:
            comps.update(new_comps)
            new_comps = set.union(*(d.parents for d in new_comps)) - comps
        return comps

    @staticmethod
    def _topological_sort(comps):
        comps = list(comps)
        comps.sort()
        return comps

@attr.s
class Resource:
    loc = attr.ib()
    digest = attr.ib()

    @id_property
    def resource_id(self):
        return {
            'loc': self.loc,
            'digest': self.digest
        }

@attr.s
class Calc:
    inputs = attr.ib(validator=instance_of(tuple))
    op = attr.ib()

    @id_property
    def calc_id(self):
        return {
            'inputs': [inp.resource_id for inp in self.inputs],
            'op': self.op
        }

@attr.s
class User:
    user_id = attr.ib()
    name = attr.ib()


@attr.s
class Run:
    run_id = attr.ib()
    calc = attr.ib()
    results = attr.ib(validator=instance_of(tuple))
    start_time = attr.ib()
    end_time = attr.ib()
    user = attr.ib()
