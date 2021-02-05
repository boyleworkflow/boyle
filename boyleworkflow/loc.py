from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple

_SEPARATOR = "/"
_DOT = "."
_DOUBLE_DOT = ".."
_FORBIDDEN_NAMES = ["", _DOT, _DOUBLE_DOT]


@dataclass(frozen=True, order=True)
class Name:
    value: str

    def __post_init__(self):
        if self.value in _FORBIDDEN_NAMES:
            raise ValueError(f"invalid Name: {repr(self.value)}")
        if _SEPARATOR in self.value:
            raise ValueError(f"invalid Name with separator: {self.value}")


@dataclass(frozen=True, order=True)
class Loc:
    names: Tuple[Name, ...] = ()

    @classmethod
    def from_string(cls, value: str) -> Loc:
        if value.endswith(_SEPARATOR):
            value = value[:-1]
        loc_segments = (v for v in value.split(_SEPARATOR) if v != _DOT)
        return Loc(tuple(map(Name, loc_segments)))

    def to_string(self) -> str:
        if self.names:
            return _SEPARATOR.join((n.value for n in self.names))
        else:
            return _DOT

    def __truediv__(self, name: Name) -> Loc:
        return Loc(self.names + (name,))

    @property
    def parent(self):
        return Loc(self.names[:-1])

    def __repr__(self):
        return f"Path('{self.to_string()}')"
