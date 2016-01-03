import click
from gpc import *
import gpc
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
    user = gpc.config['user']
    log = Log(DEFAULT_LOG_PATH, user)
    storage = Storage(DEFAULT_STORAGE_PATH)
    graph = graph_from_spec('gpc.yaml')

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

@main_group.group()
def config():
    pass

def name_param(f):
    def validate(ctx, param, name):
        if not '.' in name:
            raise click.BadParameter(
                "gpc config items are named like 'section.item'")
        return name

    decorator = click.argument('name', callback=validate)
    return decorator(f)

@config.command()
@click.option('--local', 'file', flag_value='local', default=True)
@click.option('--global', 'file', flag_value='global')
@name_param
@click.argument('value')
def set(file, name, value):
    section, item = name.split('.', 1)
    gpc.gpc.set_config(file, section, item, value)


@config.command()
@name_param
def get(name):
    section, item = name.split('.', 1)
    try:
        click.echo(gpc.gpc.config[section][item])
    except KeyError:
        raise click.BadParameter("config item '{}' is not set".format(name))
