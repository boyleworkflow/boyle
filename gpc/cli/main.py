#! /usr/bin/env python
import click
from gpc.gpc import *
from gpc import spec_reader
from gpc.tests import default_user
import shutil

DEFAULT_LOG_PATH = 'log'
DEFAULT_STORAGE_PATH = 'storage'

@click.group()
def main_group():
    pass

@main_group.command()
@click.argument('target', nargs=-1)
def make(target):
    '''
    Make target files.

    Run necessary calculations to generate the target
    files. If the target files already exist in cache, simply copy them into
    working directory.'''
    user = default_user
    log = Log(DEFAULT_LOG_PATH, user)
    storage = Storage(DEFAULT_STORAGE_PATH)
    graph = spec_reader.graph_from_spec('gpc.yaml')

    runner = Runner(log, storage, graph)
    for t in list(target):
        runner.make(t)

        responsible_runs = log.get_provenance(digest_file(t))
        print('The file was produced by %i run(s):' % len(responsible_runs))
        for r in responsible_runs:
            print(r)

@main_group.command()
def init():
    '''
    Init in the current directory.
    '''
    log_created = False
    storage_created = False
    try:
        Log.create(DEFAULT_LOG_PATH)
        log_created = True
        Storage.create(DEFAULT_STORAGE_PATH)
        storage_created = True
    except Exception as e:
        if log_created:
            shutil.rmtree(DEFAULT_LOG_PATH)
        if storage_created:
            shutil.rmtree(DEFAULT_STORAGE_PATH)
        raise e

