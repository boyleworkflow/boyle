from functools import wraps

import attr

@attr.s
class Task:

    out = attr.ib()
    inp = attr.ib()
    op = attr.ib()

@attr.s
class Output:

    task = attr.ib()
    out_idx = attr.ib()


def inp(*input_locs):

    def attach_inp(get_op):

        @wraps(get_op)
        def wrapper(*args, **kwargs):
            args = args[len(input_locs):]
            return get_op(*args, **kwargs)

        wrapper.inp = input_locs

        return wrapper

    return attach_inp


def out(*output_locs):

    def attach_out(get_op):

        @wraps(get_op)
        def build_task(*args, **kwargs):
            op = get_op(*args, **kwargs)
            task = Task(out=output_locs, inp=get_op.inp, op=op)
            outputs = [Output(task, i) for i in range(len(output_locs))]
            return outputs

        return build_task

    return attach_out


@out('hej', 'hå')
@inp('inp1', 'inp2')
def do_something(**kwargs):
    return f'Operation[{kwargs}]'

if __name__ == '__main__':
    f, g = do_something('hejsan', 'svejsan', extra='xyz')
    print(f)
    print(g)
