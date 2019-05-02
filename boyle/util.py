import functools
import json
import hashlib


digest_func = hashlib.sha1


def digest_str(s):
    return digest_func(s.encode('utf-8')).hexdigest()


def unique_json(obj):
    return json.dumps(obj, sort_keys=True)


def digest_file(path):
    with open(path, 'rb') as f:
        return digest_func(f.read()).hexdigest()
