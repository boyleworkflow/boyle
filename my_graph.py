# -*- coding: utf-8 -*-

import gpc

a = gpc.FileTarget('data/a')
b = gpc.FileTarget('data/b')

t1 = gpc.PythonTask([], [a], 'calc.task1')
t2 = gpc.PythonTask([a], [b], 'calc.task2')


