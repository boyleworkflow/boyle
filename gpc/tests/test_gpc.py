import unittest
from gpc.gpc import *
import networkx

class TestGraph(unittest.TestCase):
    def test_add_task(self):
        g = Graph()
        outputs = ['a']
        task = ShellTask('command', [], outputs)
        g.add_task(task)
        self.assertTrue(task in g.get_tasks(outputs[0]))
        self.assertTrue(len(g.get_tasks(outputs[0])) == 1)

    def test_add_task_clashing_output_name(self):
        g = Graph()
        outputs = ['a', 'b', 'c']
        g.add_task(ShellTask('command', [], outputs[0:2]))
        with self.assertRaises(ValueError):
            g.add_task(ShellTask('command2', [], outputs[1:3]))
        self.assertTrue(len(g.get_tasks(outputs[0])) == 1)
        self.assertTrue(len(g.get_tasks(outputs[1])) == 1)
        with self.assertRaises(networkx.exception.NetworkXError):
            g.get_tasks(outputs[2])

    def test_ensure_complete(self):
        g = Graph()
        g.add_task(ShellTask('command', ['a'], ['b']))
        self.assertTrue(len(g.get_tasks('b')) == 1)
        g.ensure_complete()
        self.assertTrue(len(g.get_tasks('b')) == 2)

        g = Graph()
        g.add_task(ShellTask('command', [], ['a']))
        self.assertTrue(len(g.get_tasks('a')) == 1)
        g.ensure_complete()
        self.assertTrue(len(g.get_tasks('a')) == 1)
            
if __name__ == '__main__':
    unittest.main()
