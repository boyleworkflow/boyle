from dataclasses import dataclass
from typing import Iterator, Mapping, TypeVar

KT = TypeVar("KT")
VT = TypeVar("VT", covariant=True)


@dataclass(init=False, frozen=True)
class FrozenDict(Mapping[KT, VT]):
    _data: Mapping[KT, VT]

    def __init__(self, data: Mapping[KT, VT]):
        data = dict(data.items())
        object.__setattr__(self, "_data", data)

    def __getitem__(self, key: KT) -> VT:
        return self._data[key]

    def __iter__(self) -> Iterator[KT]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __hash__(self) -> int:
        return hash(frozenset(self._data.items()))

    def __repr__(self) -> str:
        return f"FrozenDict({repr(self._data)})"

    def __str__(self) -> str:
        return f"FrozenDict({str(self._data)})"
