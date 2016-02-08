import click
import gpc
import shutil
import gpc.config
import yaml
import json

settings = gpc.config.load()

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
    working directory.
    '''
    user = dict(name=settings['user.name'], id=settings['user.id'])
    log = gpc.Log(DEFAULT_LOG_PATH, user)
    storage = gpc.Storage(DEFAULT_STORAGE_PATH)
    graph = gpc.graph_from_spec('gpc.yaml')

    runner = gpc.Runner(log, storage, graph)
    for t in list(target):
        runner.make(t)

        responsible_runs = log.get_provenance(gpc.digest_file(t))
        print('The file was produced by %i run(s):' % len(responsible_runs))
        for r in responsible_runs:
            print(r)

    log.write()

@main_group.command()
def init():
    '''
    Init in the current directory.
    '''
    log_created = False
    storage_created = False
    try:
        gpc.Log.create(DEFAULT_LOG_PATH)
        log_created = True
        gpc.Storage.create(DEFAULT_STORAGE_PATH)
        storage_created = True
    except Exception as e:
        if log_created:
            shutil.rmtree(DEFAULT_LOG_PATH)
        if storage_created:
            shutil.rmtree(DEFAULT_STORAGE_PATH)
        raise e

@main_group.group()
def config():
    '''
    Get or set configuration options.
    '''
    pass

def key_param(f):
    decorator = click.argument('key')
    return decorator(f)

def value_param(f):
    def validate(ctx, param, value):
        try:
            value = yaml.safe_load(value)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:
            msg = 'The value is not valid YAML.\n\n{}\n{}'.format(
                e.problem, e.problem_mark)
            raise click.BadParameter(msg)
        return value

    decorator = click.argument('value', callback=validate)
    return decorator(f)

@config.command()
@click.option('--local', 'file', flag_value='?local')
@click.option('--global', 'file', flag_value='?global', default=True)
@key_param
@value_param
def set(file, key, value):
    """
    Set a configuration item.

    The key can be any string. The value is represented as YAML
    in the config file and this command fails if that can't be done.
    """
    gpc.config.set(file, key, value)


@config.command()
@click.option('--local', 'file', flag_value='?local')
@click.option('--global', 'file', flag_value='?global', default=True)
@key_param
def unset(file, key):
    """Remove a configuration item."""
    try:
        gpc.config.unset(file, key)
    except KeyError:
        raise click.BadParameter(
            "config item '{}' is not set ({} file)".format(key, file))


@config.command()
@key_param
@click.option(
    '--output-format', '-f',
    type=click.Choice(['yaml', 'json']), default='yaml')
def get(key, output_format):
    """
    Get a configuration item.

    The value is output on stdout. Use the -f flag to choose
    between YAML and JSON output.
    """
    try:
        value = settings[key]
    except KeyError:
        raise click.BadParameter(
            "config item '{}' is not set ({} file)".format(key, file))

    if output_format == 'yaml':
        value = yaml.safe_dump(value, indent=2, default_flow_style=False)
    elif output_format == 'json':
        value = json.dumps(value)

    click.echo(value)

@config.command()
@click.option(
    '--output-format', '-f',
    type=click.Choice(['yaml', 'json']), default='yaml')
def lst(output_format):
    """List all configuration items."""
    value = settings

    if output_format == 'yaml':
        value = yaml.safe_dump(value, indent=2, default_flow_style=False)
    elif output_format == 'json':
        value = json.dumps(value)

    click.echo(value)
