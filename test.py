import prov
import yaml

graph_path = 'graph.yml'
with open(graph_path, 'r') as f:
    graph = yaml.load(f.read())
    print(prov.get(graph, 'd.value'))