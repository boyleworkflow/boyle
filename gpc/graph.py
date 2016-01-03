import networkx as nx
from gpc.gpc import ShellTask, Task, CopyTask

class GraphError(Exception): pass


class Graph(object):
    def __init__(self):
        super(Graph, self).__init__()
        self._graph = nx.DiGraph()
        self._tasks = set()
        self._paths = set()

    def add_task(self, task):
        if any(path in self._graph for path in task.outputs):
            raise GraphError('duplicate outputs not allowed')

        for path in task.inputs:
            self._graph.add_edge(path, task)

        for path in task.outputs:
            self._graph.add_edge(task, path)

        self._paths.update(task.outputs)
        self._tasks.add(task)

    def get_task(self, output_path):
        if not output_path in self._graph:
            raise GraphError("output '{}' not in graph".format(output_path))
        pred = self._graph.predecessors(output_path)
        if len(pred) == 0:
            raise GraphError("no task has '{}' as output".format(output_path))
        elif len(pred) == 1:
            return pred[0]
        else:
            raise RuntimeError('this should not happen')

    def get_upstream_paths(self, *requested_paths):
        subgraph_members = set(requested_paths)
        for path in requested_paths:
            subgraph_members.update(nx.ancestors(self._graph, path))

        subgraph_paths = self._paths.intersection(subgraph_members)
        full_subgraph = nx.subgraph(self._graph, subgraph_members)
        path_subgraph = nx.projected_graph(full_subgraph, subgraph_paths)
        return(nx.topological_sort(path_subgraph))

    def ensure_complete(self):
        ## find input at start of graph
        ## add CopyTask for each such input
        for node in nx.nodes(self._graph):
            if len(nx.ancestors(self._graph, node)) == 0:
                if not isinstance(node, Task):
                    t = CopyTask(node)
                    self._graph.add_edge(t, node)
                    self._paths.update(node)
                    self._tasks.add(t)
