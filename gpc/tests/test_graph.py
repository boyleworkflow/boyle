import unittest
from gpc.graph import *

class TestGraph(unittest.TestCase):
    def test_add_task(self):
        g = Graph()
        outputs = ['a']
        task = ShellTask('command', [], outputs)
        g.add_task(task)
        self.assertTrue(g.get_task(outputs[0]) is task)
        self.assertTrue(len(g.get_upstream_paths(outputs[0])) == 1)

    def test_add_task_clashing_output_name(self):
        g = Graph()
        outputs = ['a', 'b', 'c']
        g.add_task(ShellTask('command', [], outputs[0:2]))
        with self.assertRaises(GraphError):
            g.add_task(ShellTask('command2', [], outputs[1:3]))
        self.assertTrue(len(g.get_upstream_paths(outputs[0])) == 1)
        self.assertTrue(len(g.get_upstream_paths(outputs[1])) == 1)
        with self.assertRaises(GraphError):
            g.get_task(outputs[2])

    def test_ensure_complete(self):
        g = Graph()
        g.add_task(ShellTask('command', ['a'], ['b']))
        with self.assertRaises(GraphError):
            g.get_task('a')
        g.ensure_complete()
        self.assertTrue(isinstance(g.get_task('a'), CopyTask))

        g = Graph()
        g.add_task(ShellTask('command', [], ['a']))
        self.assertTrue(list(g.get_upstream_paths('a')) == ['a'])
        g.ensure_complete()
        self.assertTrue(list(g.get_upstream_paths('a')) == ['a'])

    def test_get_upstream_paths(self):
        g = Graph()
        t1 = ShellTask('c1', ['a', 'b'], ['c'])
        g.add_task(t1)
        t2 = ShellTask('c2', ['a', 'd'], ['e'])
        g.add_task(t2)
        g.ensure_complete()
        lc = g.get_upstream_paths('c')
        le = g.get_upstream_paths('e')
        self.assertTrue(set(lc) == {'a', 'b', 'c'})
        self.assertTrue(lc[-1] == 'c')
        self.assertTrue(set(le) == {'a', 'd', 'e'})
        self.assertTrue(le[-1] == 'e')

        t3 = ShellTask('c3', ['c', 'e'], ['f'])
        g.add_task(t3)
        lf = g.get_upstream_paths('f')
        self.assertTrue(set(lf) == {'a', 'b', 'c', 'd', 'e', 'f'})
        self.assertTrue(set(lf[0:5]) == {'a', 'b', 'c', 'd', 'e'})
        self.assertTrue('a' in lf[0:3])
        self.assertTrue('c' in lf[2:5])
        self.assertTrue('e' in lf[2:5])

if __name__ == '__main__':
    unittest.main()
