import unittest
import tempfile
import os
import datetime
from itertools import product, combinations_with_replacement
import boyle

class TestComp(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple_1(self):
        a = boyle.Comp(parents=(), op='cat "hello " > file_a', loc='file_a')
        b = boyle.Comp(parents=(), op='cat "world" > file_b', loc='file_b')
        c = boyle.Comp(
            parents=(a, b),
            op='cat file_a file b > file_c',
            loc='file_c')

        self.assertEqual({a}, boyle.Comp.get_ancestors([a]))
        self.assertEqual({b}, boyle.Comp.get_ancestors([b]))
        self.assertEqual({a, b, c}, boyle.Comp.get_ancestors([c]))



if __name__ == '__main__':
    unittest.main()
