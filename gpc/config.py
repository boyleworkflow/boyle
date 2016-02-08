import os
import yaml
import logging

GLOBAL_PATH = os.path.expanduser('~/.config/gpc/config.yml')
# This should be relative! To follow along if one changes working directory.
LOCAL_PATH = '.gpc/config.yml'

def _read_config_if_exists(path):
    if not os.path.exists(path):
        return {}

    with open(path, 'r') as f:
        return dict(yaml.safe_load(f))

def _update_config(old, new):
    for key, item in new.items():

        # If the key already exists and both the old and the new items
        # are subsections, then we write into the old one recursively.

        if isinstance(item, dict) and isinstance(old.get(key, None), dict):
            _update_config(old[key], item)

        # Otherwise, just overwrite
        else:
            old[key] = item

def load_settings():
    config = {}
    for path in (GLOBAL_PATH, LOCAL_PATH):
        new = _read_config_if_exists(path)
        _update_config(config, new)
    return config

def _update_at_location(config, location, value):
    parts = location.split('.')
    assert all(p.isalpha() for p in parts)
    levels, item = parts[:-1], parts[-1]
    
    # Descend to the right sub(sub-sub-sub-...)section
    section = config
    for level in levels:
        if not level in section:
            section[level] = {}
        section = section[level]

    section[item] = value

    return config

def get_location(config, location):
    parts = location.split('.')
    assert all(p.isalpha() for p in parts)
    
    # Descend to the right sub(sub-sub-sub-...)section
    section = config
    levels, item = parts[:-1], parts[-1]
    for level in levels:
        section = section[level]

    return section[item]


def set_config(file, location, value):
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
    _update_at_location(config, location, value)

    with open(path, 'w') as configfile:
        yaml.safe_dump(config, configfile, indent=2, default_flow_style=False)
