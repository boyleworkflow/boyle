#! /usr/bin/env python
import click
from gpc.gpc import *
from gpc import spec_reader
from gpc.tests import default_user

@click.group()
def main_group():
    pass

@main_group.command()
@click.argument('target', nargs=-1)
def make(target):
    '''Make target files. Run necessary calculations to generate the target
    files. If the target files already exist in cache, simply copy them into
    working directory.'''
    user = default_user
    log = Log('log', user)
    storage = Storage('storage')
    graph = spec_reader.graph_from_spec('gpc.yaml')

    runner = Runner(log, storage, graph)
    for t in list(target):
        runner.make(t)

        responsible_runs = log.get_provenance(digest_file(t))
        print('The file was produced by %i run(s):' % len(responsible_runs))
        for r in responsible_runs:
            print(r)
