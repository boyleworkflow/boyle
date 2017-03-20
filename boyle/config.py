import os
import json
import logging
import pkg_resources

DEFAULT_PATH = os.path.abspath(
    pkg_resources.resource_filename(__name__, 'resources/boyleconfig.json'))
GLOBAL_PATH = os.path.expanduser('~/.config/boyle/boyleconfig.json')

# This should be relative! To follow along if one changes the working directory.
LOCAL_PATH = 'boyleconfig.json'

def _read_config_if_exists(path):
    if not os.path.exists(path):
        return {}

    with open(path, 'r') as f:
        config = json.load(f)
        return config

def load():
    """
    Load the configuration dictionary.

    The configuration is a stupid dictionary, simply read from files.
    Changing the dictionary has no effect on the files.

    The configuration is read in sequence from the following places:

        * boyle.config.DEFAULT_PATH
        * boyle.config.GLOBAL_PATH
        * boyle.config.LOCAL_PATH

    Each read overrides previously defined values.

    """

    config = {}
    for path in (DEFAULT_PATH, GLOBAL_PATH, LOCAL_PATH):
        config.update(_read_config_if_exists(path))
    return config

def _get_config_path(path):
    if path == '?local':
        path = LOCAL_PATH
    elif path == '?global':
        path = GLOBAL_PATH

    return path

def _load_config_file(path):
    path = _get_config_path(path)
    config = _read_config_if_exists(path)
    return config

def _overwrite_config_file(path, config):
    path = _get_config_path(path)

    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    with open(path, 'w') as configfile:
        json.dump(config, configfile, indent=4)
        configfile.write(os.linesep)


def set(path, key, value):
    """
    Set a value in the configuration dictionary.

    Args:
        path (str): The config file to alter. The values ?local and ?global
            are treated specially: they are changed to
            boyle.config.LOCAL_PATH and boyle.config.GLOBAL_PATH, respectively.
        key (str): The config item to change.
        value: Anything json-encodable.

    """
    config = _load_config_file(path)
    config[key] = value
    _overwrite_config_file(path, config)


def unset(path, key):
    """
    Remove a value from the configuration dictionary.

    Args:
        path (str): The config file to alter. The values ?local and ?global
            are treated specially: they are changed to
            boyle.config.LOCAL_PATH and boyle.config.GLOBAL_PATH, respectively.
        key (str): The config item to remove.

    Raises:
        IOError: If file does not exist.
        KeyError: If key does not exist.

    """
    path = _get_config_path(path)
    if not os.path.exists(path):
        raise IOError('The file {} does not exist.'.format(path))

    config = _load_config_file(path)
    del config[key]

    _overwrite_config_file(path, config)
