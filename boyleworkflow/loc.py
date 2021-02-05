from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence, Tuple

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


@dataclass(frozen=True, order=True, init=False)
class Loc:
    names: Tuple[Name, ...] = ()

    def __init__(self, value: str = _DOT):
        if value.endswith(_SEPARATOR):
            value = value[:-1]
        name_strs = (v for v in value.split(_SEPARATOR) if v != _DOT)
        names = tuple(map(Name, name_strs))
        object.__setattr__(self, "names", names)

    @staticmethod
    def _str_from_names(names: Sequence[Name]) -> str:
        if names:
            return _SEPARATOR.join((n.value for n in names))
        else:
            return _DOT

    @staticmethod
    def from_names(names: Sequence[Name]) -> Loc:
        return Loc(Loc._str_from_names(names))

    def __str__(self) -> str:
        return Loc._str_from_names(self.names)

    def __truediv__(self, name: Name) -> Loc:
        return Loc.from_names(self.names + (name,))

    @property
    def parent(self):
        return Loc.from_names(self.names[:-1])

    def __repr__(self):
        return f"Path('{str(self)}')"
