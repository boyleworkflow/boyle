from typing import Union, Any
import os
import stat
import hashlib
import json
from pathlib import Path
import functools

PathLike = Union[Path, str]

### FILE PERMISSIONS

BITS = {
    "write": [stat.S_IWUSR, stat.S_IWGRP, stat.S_IWOTH],
    "read": [stat.S_IRUSR, stat.S_IRGRP, stat.S_IROTH],
    "execute": [stat.S_IXUSR, stat.S_IXGRP, stat.S_IXOTH],
}


def _enable(permissions, bit):
    return permissions | bit


def _disable(permissions, bit):
    return permissions & (~bit)


def set_file_permissions(path, read=None, write=None, execute=None):
    permissions = stat.S_IMODE(os.stat(path).st_mode)

    choices = dict(read=read, write=write, execute=execute)

    for k, v in choices.items():
        if v is None:
            continue

        operation = _enable if v else _disable

        for bit in BITS[k]:
            permissions = operation(permissions, bit)

    os.chmod(path, permissions)


def get_file_permissions(path):
    permissions = stat.S_IMODE(os.stat(path).st_mode)

    booleans = {}
    for k, bits in BITS.items():
        booleans[k] = tuple((permissions & bit) > 0 for bit in bits)

    return booleans


### DIGESTS ETC

digest_func = hashlib.sha1


def digest_str(s: str) -> str:
    return digest_func(s.encode("utf-8")).hexdigest()


_CHUNK_SIZE = 1024


def digest_file(path: PathLike) -> str:
    digest = digest_func()
    with open(path, "rb") as f:
        while True:
            data = f.read(_CHUNK_SIZE)
            if not data:
                break
            digest.update(data)
        return digest.hexdigest()


def unique_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True)


def id_property(func):
    @property
    @functools.wraps(func)
    def id_func(self):
        try:
            return self._id_str
        except AttributeError:
            pass

        id_obj = func(self)
        try:
            json = unique_json(id_obj)
        except TypeError as e:
            msg = f"The id_obj of {self} is not JSON serializable: {id_obj}"
            raise TypeError(msg) from e
        id_obj = {"type": type(self).__qualname__, "id_obj": id_obj}
        id_str = digest_str(json)
        object.__setattr__(self, "_id_str", id_str)
        return id_str

    return id_func
