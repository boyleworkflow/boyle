# -*- coding: utf-8 -*-

import os
import hashlib
import shutil
import subprocess

output_digests = {}

def copy_from_archive(digest, dst_path):
    src_path = archive_path(digest)
    shutil.copy2(src_path, dst_path)

def digest(obj):
    return hashlib.sha1(str(obj).encode('utf-8')).hexdigest()

def digest_file(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()

def archive_path(file_digest):
    return os.path.join('.gpc/archive/', file_digest)

class Graph(object):
    """docstring for Graph"""
    def __init__(self):
        super(Graph, self).__init__()
        self.tasks = {}

    def add_task(self, task):
        if any(output in self.tasks for output in task.outputs):
            raise ValueError('duplicate outputs not allowed')
        for output in task.outputs:
            self.tasks[output] = task

    def find_task(self, output):
        return self.tasks[output]

    def calc_id(self, task):
        things = tuple(task.task_id(),) + tuple((inp, self.output_digest(inp)) for inp in task.inputs)
        return digest(things)

    def output_digest(self, output):
        task = self.find_task(output)
        calc_id = self.calc_id(task)
        return output_digests[calc_id][output]

    # def file_id(self, output):
    #     task = self.find_task(output)
    #     things = (self.calc_id(task), output)
    #     return digest(things)

    def ensure_exists(self, output):
        task = self.find_task(output)
        for inp in task.inputs:
            self.ensure_exists(inp)
        
        calc_id = self.calc_id(task)
        
        if calc_id in output_digests:
            file_digest = output_digests[calc_id]
            if os.path.exists(archive_path(file_digest)):
                return
    
        workdir = os.path.join('/tmp/.gpc/', calc_id)
        if os.path.exists(workdir):
            print('deleting', workdir)
            shutil.rmtree(workdir)
        os.makedirs(workdir)
        for inp in task.inputs:
            copy_from_archive(self.output_digest(inp), os.path.join(workdir, inp))
        
        print('running in', workdir)
        task.run(workdir)

        
        output_digests[calc_id] = {}
        for output in task.outputs:
            dst_path = os.path.join(workdir, output)
            digest = digest_file(dst_path)
            if not os.path.exists(archive_path('')):
                os.makedirs(archive_path(''))
            shutil.move(dst_path, archive_path(digest))
            output_digests[calc_id][output] = digest

        if os.path.exists(workdir):
            print('deleting', workdir)
            shutil.rmtree(workdir)


    def make(self, output):
        self.ensure_exists(output)
        copy_from_archive(self.output_digest(output), output)


class ShellTask(object):
    """docstring for ShellTask"""
    def __init__(self, command, inputs, outputs):
        super(ShellTask, self).__init__()
        self.command = command
        self.inputs = inputs
        self.outputs = outputs

    def task_id(self):
        return self.command

    def run(self, workdir):
        original_wd = os.getcwd()
        os.chdir(workdir)
        try:
            subprocess.call(self.command, shell=True)
        except Exception, e:
            raise e
        finally:
            os.chdir(original_wd)
        

def main():
    t1 = ShellTask('echo hello > a', [], ['a'])
    t2 = ShellTask('echo world > b', [], ['b'])
    t3 = ShellTask('cat a b > c', ['a', 'b'], ['c'])
    g = Graph()
    g.add_task(t1)
    g.add_task(t2)
    g.add_task(t3)
    g.make('c')


if __name__ == '__main__':
    main()