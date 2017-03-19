import functools
import json
import hashlib
import attr

digest_func = hashlib.sha1

def digest_str(s):
    return digest_func(s.encode('utf-8')).hexdigest()

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)


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
    resources = attr.ib()


@attr.s
class Definition:
    instr = attr.ib()
    parents = attr.ib()
    task = attr.ib()

    def __attrs_post_init__(self):
        self.parents = tuple(self.parents)

    def __lt__(self, other_def):
        my_ancestors = _get_ancestors([self])
        return other_def not in my_ancestors

    @id_property
    def def_id(self):
        return {
            'instr': self.instr.instr_id,
            'parents': [p.def_id for p in self.parents],
            'task': self.task.task_id
        }

    @staticmethod
    def _get_ancestors(defs):
        defs = set()
        new_defs = set(defs)
        while new_defs:
            defs.update(new_defs)
            new_defs = set.union(*(d.parents for d in new_defs)) - defs
        return defs

    @staticmethod
    def _topological_sort(defs):
        defs = list(defs)
        defs.sort()
        return defs

@attr.s
class Resource:
    instr = attr.ib()
    digest = attr.ib()

    @id_property
    def resource_id(self):
        return {
            'instr': self.instr.instr_id,
            'digest': self.digest
        }

@attr.s
class Calculation:
    inputs = attr.ib()
    task = attr.ib()

    def __attrs_post_init__(self):
        self.inputs = tuple(self.inputs)

    @id_property
    def calc_id(self):
        return {
            'inputs': [inp.resource_id for inp in self.inputs],
            'task': self.task.task_id
        }

@attr.s
class User:
    user_id = attr.ib()
    name = attr.ib()


@attr.s
class Run:
    run_id = attr.ib(default=None)
    calc = attr.ib()
    results = attr.ib()
    start_time = attr.ib()
    end_time = attr.ib()
    user = attr.ib()

    def __attrs_post_init__(self):
        self.results = tuple(self.results)
        if self.run_id == None:
            self.run_id = str(uuid.uuid4())

