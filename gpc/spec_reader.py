import yaml
from gpc.gpc import *

def graph_from_spec(yaml_file):
    return graph_from_str(open(yaml_file, 'r'))

def graph_from_str(yaml_str):
    g = Graph()
    graph_spec = yaml.load(yaml_str)

    for task in graph_spec['tasks']:
        if 'shell' in task:
            t = ShellTask(task['shell'],
                          task.get('inputs', []),
                          task.get('outputs', []))
            g.add_task(t)

    return g
