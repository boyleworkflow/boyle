# -*- coding: utf-8 -*-

cache = {} # maps data digests to data
previous_executions = {} # maps task executions to (output name, data_digest) tuples

class Output(object):
    """docstring for Output"""
    def __init__(self, name, task):
        super(Output, self).__init__()
        self.name = name
        self.task = task

    def vid(self):
        eid = self.task.eid()
        if not eid in previous_executions:
            self.task.run()
        return previous_executions[eid][self.name]

    def value(self):
        vid = self.vid()
        if not vid in cache:
            self.task.run()
        return cache[vid]

class Task(object):
    """docstring for Task"""
    def __init__(self, func, inputs, output_names):
        super(Task, self).__init__()
        self.func = func
        self.inputs = inputs
        self.outputs = {name: Output(name, self) for name in output_names}

    def eid(self):
        return (self.func,) + tuple((inp.name, inp.vid()) for inp in self.inputs)


    def run(self):
        eid = self.eid()

        try:
            vids = previous_executions[eid]
            values = {output_name: cache[vid] for output_name, vid in vids.items()}
        except KeyError:
            values = self.func(**{inp.name: inp.value() for inp in self.inputs})
            vids = {output_name: hash(value) for output_name, value in values.items()}
            previous_executions[eid] = vids
            for output_name, vid in vids.items():
                cache[vid] = values[output_name]

        return values

def main():
    f1 = lambda: {'a': 42}
    f2 = lambda **d: {'b': d['a'] + 1}
    f3 = lambda **d: {'c': d['a'] + d['b']}

    t1 = Task(f1, [], ['a'])
    t2 = Task(f2, [t1.outputs['a']], ['b'])
    t3 = Task(f3, [t1.outputs['a'], t2.outputs['b']], ['c'])

    print(t3.outputs['c'].value())

if __name__ == '__main__':
    main()