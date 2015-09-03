# -*- coding: utf-8 -*-

import sys
import os
import tempfile
import importlib
import re
from modulefinder import ModuleFinder

class PythonTask(object):
    """docstring for PythonTask"""
    def __init__(self, depends, creates, func_name):
        for target in creates:
            target.task = self
        self.depends = depends
        self.creates = creates
        self.func_name = func_name

    def __str__(self):
        return '{} ({})'.format(repr(self), self.func_name)

    @property
    def modules(self):
        # We can and should probably just steal Sumatra's implementation instead,
        # but the following catches the gist of it:
        try:
            return self._modules
        except AttributeError:
            with tempfile.NamedTemporaryFile('w', delete=False) as file:
                path = file.name
                file.write("import {}".format(self.func_name))
            finder = ModuleFinder()
            finder.run_script(path)
            os.remove(path)
            self._modules = {k: m.__file__ for k, m in finder.modules.items()
                             if not k == '__main__'}
            return self._modules

class FileTarget(object):
    """docstring for FileTarget"""
    def __init__(self, path):
        self.path = path
        self._task = None

    def __str__(self):
        return '{} ({})'.format(repr(self), self.path)

    @property
    def task(self):
        return self._task

    @task.setter
    def task(self, value):
        if self._task is not None:
            raise Exception('this target already belongs to a task')
        self._task = value
    

# gpc make [target]:
#     find out what is needed in directory, current state of system, etc
#     from that, calculate hash/id of the requested target
#     if not cached target exists:
#         setup working directory
#         start separate process to run the task that directory
#         wait
#         save record
#         cache target
#         clean/remove working directory
#         
#     when target exists:
#         present it to user

def main():
    if sys.argv[1] == 'make':
        target_path = re.match('(?P<module>.+)\.(?P<target>[^\.]+)', sys.argv[2])
        graph_mod = importlib.import_module(target_path.group('module'))
        target = getattr(graph_mod, target_path.group('target'))
        print('Target:\t{}\n\nTask:\t{}'.format(target, target.task))
        
        print('\nDepends on Target(s):')
        for t in target.task.depends:
            print('\t{}'.format(t))
        print('\nDepends on module(s):')
        for m, p in target.task.modules.items():
            print('\t{}: {}'.format(m, p))

if __name__ == '__main__':
    main()