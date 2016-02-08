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
        config = yaml.safe_load(f.read())
        return config if config else {}

def load():
    """
    Load the configuration dictionary.

    The configuration is a stupid dictionary, simply read from files.
    Changing the dictionary has no effect on the files.

    The configuration is read in sequence from the following places:

        * gpc.config.DEFAULT_PATH
        * gpc.config.GLOBAL_PATH
        * gpc.config.LOCAL_PATH

    Each read overrides previously defined values.

    """

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
        value: Anything PyYAML can represent as a string. In other
            words, at least all combinations of dict, list, string and
            numeric literals.

    Raises:
        yaml.representer.RepresenterError: If the value cannot be represented
            as YAML.

    """
    test_dump = yaml.safe_dump(value)

    if path == '?local':
        path = LOCAL_PATH
    elif path == '?global':
        path = GLOBAL_PATH

    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    config = _read_config_if_exists(path)
    config[key] = value

    with open(path, 'w') as configfile:
        yaml.safe_dump(config, configfile, indent=2, default_flow_style=False)

def unset(path, key):
    """
    Remove a value from the configuration dictionary.

    Args:
        path (str): The config file to alter. The values ?local and ?global
            are treated specially: they are changed to
            gpc.config.LOCAL_PATH and gpc.config.GLOBAL_PATH, respectively.
        key (str): The config item to remove.

    Raises:
        IOError: If file does not exist.
        KeyError: If key does not exist.

    """
    if path == '?local':
        path = LOCAL_PATH
    elif path == '?global':
        path = GLOBAL_PATH

    if not os.path.exists(path):
        raise IOError('The file {} does not exist.'.format(path))

    config = _read_config_if_exists(path)
    del config[key]

    with open(path, 'w') as configfile:
        yaml.safe_dump(config, configfile, indent=2, default_flow_style=False)
