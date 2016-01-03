#! /usr/bin/env python
import click
import gpc
from gpc import spec_reader
from gpc.log import *
from gpc.storage import *
from gpc.runner import *
from gpc.gpc import digest_file
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
    user = gpc.gpc.config['user']
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

SETTINGS = {
    'user.name': str,
    'user.id': str
}

@main_group.group()
def config():
    pass

@config.command()
@click.option('--local', 'file', flag_value='local', default=True)
@click.option('--global', 'file', flag_value='global')
@click.argument('name')
@click.argument('value')
def set(file, name, value):
    path = (
        LOCAL_CONFIG_FILE_PATH if file == 'local' else GLOBAL_CONFIG_FILE_PATH)

    if name not in SETTINGS:
        raise click.BadParameter("unknown name '{}'".format(name))

    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    configuration = configparser.ConfigParser()
    configuration.read(path)

    section, item = name.split('.', 1)
    if section not in configuration:
        configuration.add_section(section)
    configuration[section][item] = value

    with open(path, 'w') as configfile:
        configuration.write(configfile)
