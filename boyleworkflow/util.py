from __future__ import annotations
import hashlib
import json
from typing import Any, Mapping, Optional, Sequence, Tuple, Type, Union
from boyleworkflow.frozendict import FrozenDict


_ID_HASH = hashlib.sha256

_json_atoms = (str, int, bool, float)
JSONAtom = Optional[Union[str, int, bool, float]]
JSONData = Union[JSONAtom, Sequence["JSONData"], Mapping[str, "JSONData"]]
FrozenJSON = Union[JSONAtom, Tuple["FrozenJSON", ...], FrozenDict[str, "FrozenJSON"]]

def unfreeze(obj: JSONData) -> JSONData:
    if obj is None or isinstance(obj, _json_atoms):
        return obj
    if isinstance(obj, Mapping):
        return {k: unfreeze(v) for k, v in obj.items()}

    return list(map(unfreeze, obj))

    assert False


def freeze(obj: JSONData) -> FrozenJSON:
    if obj is None or isinstance(obj, _json_atoms):
        return obj
    if isinstance(obj, Mapping):
        return FrozenDict({k: freeze(v) for k, v in obj.items()})

    return tuple(map(freeze, obj))

    assert False


def get_id_str(cls: Type[Any], obj: JSONData) -> str:
    string_representation = _get_unique_json(
        {
            "class": cls.__qualname__,
            "data": unfreeze(obj),
        }
    )
    m = _ID_HASH()
    m.update(string_representation.encode())
    return m.hexdigest()


def _get_unique_json(obj: JSONData):
    return json.dumps(obj, sort_keys=True)
