import re
import importlib
import logging

logging.basicConfig(level=logging.DEBUG)

def get(graph, name):
    path = name.split('.')

    logging.debug('requested node {}'.format(name))
    current = graph
    for child in path:
        if 'python' in current and child == 'value' and child not in current:
            current[child] = run_python(current, graph)

        if child in current:
            current = current[child]
        else:
            raise KeyError('no such node')

    return current


def run_python(node, graph):
    python_call = re.match('(?P<module>.+)\.(?P<func_name>[^\.]+)', node['python'])
    module = importlib.import_module(python_call.group('module'))
    func = getattr(module, python_call.group('func_name'))
    args = [get(graph, arg) for arg in node['args']]
    logging.debug('running python function: {}'.format(node['python']))
    return func(*args)
