from typing import NewType
from enum import Enum
from pathlib import PurePath

BASE_DIR = "files"


class SpecialFilePath(Enum):
    STDIN = "../stdin"
    STDOUT = "../stdout"
    STDERR = "../stderr"


Loc = NewType("Loc", str)


_SPECIAL_ALLOWED_LOCS = {f.value for f in SpecialFilePath}


def check_valid_loc(s: str):
    if s in _SPECIAL_ALLOWED_LOCS:
        return

    if s == "":
        raise ValueError(f"empty loc '{s}' is not allowed (try '.' instead)")

    p = PurePath(s)

    if p.is_absolute():
        raise ValueError(f"loc '{p}' is absolute")

    if p.is_reserved():
        raise ValueError(f"loc '{p}' is reserved.")

    if ".." in p.parts:
        raise ValueError(f"loc '{p}' contains disallowed '..'")


def normalize_loc(s: str) -> Loc:
    check_valid_loc(s)
    path = PurePath(s)
    return Loc(str(PurePath(*path.parts)))


def is_valid_loc(s: str) -> bool:
    try:
        check_valid_loc(s)
        return True
    except ValueError:
        return False
