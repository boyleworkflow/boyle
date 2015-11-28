# -*- coding: utf-8 -*-

import hashlib
import json

def unique_json(obj):
    return json.dumps(obj, sort_keys=True)

def hexdigest(str_or_unicode):
    return hashlib.sha1(str_or_unicode.encode('utf-8')).hexdigest()

def digest_file(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()
