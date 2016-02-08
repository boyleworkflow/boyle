import os
import yaml
import logging
import pkg_resources

DEFAULT_PATH = os.path.abspath(
    pkg_resources.resource_filename(__name__, 'resources/config.yml'))
GLOBAL_PATH = os.path.expanduser('~/.config/gpc/config.yml')

# This should be relative! To follow along if one changes the working directory.
LOCAL_PATH = '.gpc/config.yml'

def _read_config_if_exists(path):
    if not os.path.exists(path):
        return {}

    with open(path, 'r') as f:
        return dict(yaml.safe_load(f))

def load():
    config = {}
    for path in (DEFAULT_PATH, GLOBAL_PATH, LOCAL_PATH):
        config.update(_read_config_if_exists(path))
    return config


def set(path, key, value):
    """
    Set a value in the configuration dictionary.

    Args:
        path (str): The config file to alter. The values ?local and ?global
            are treated specially: they are changed to
            gpc.config.LOCAL_PATH and gpc.config.GLOBAL_PATH, respectively.
        key (str): The config item to change.
        value: Can be anything PyYAML can make into a string. In other
            words, any combination of dict, list, string or numeric literal.

    Raises:
        Some error

    """
    if path == '?local':
        path = LOCAL_PATH
    elif path == '?global':
        path = GLOBAL_PATH
    else:
        raise ValueError("there is no config file '{}'".format(path))

    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    config = _read_config_if_exists(path)
    config[key] = value

    with open(path, 'w') as configfile:
        yaml.safe_dump(config, configfile, indent=2, default_flow_style=False)

def unset(file, key):
    if file == 'local':
        path = LOCAL_PATH
    elif file == 'global':
        path = GLOBAL_PATH
    else:
        raise ValueError("there is no config file '{}'".format(file))

    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    config = _read_config_if_exists(path)
    del config[key]

    with open(path, 'w') as configfile:
        yaml.safe_dump(config, configfile, indent=2, default_flow_style=False)
