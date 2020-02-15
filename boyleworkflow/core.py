from typing import (
    Mapping,
    Union,
    Any,
    Collection,
    NewType,
    Sequence,
    Tuple,
    List,
    TypeVar,
    Type,
    Optional,
    AbstractSet,
)
from typing_extensions import Protocol
import datetime
from pathlib import Path

import attr

from boyleworkflow.util import (
    get_uuid_string,
    unique_json,
    unique_json_digest,
    digest_str,
)


JsonDict = Mapping[str, Any]
OpId = NewType("OpId", str)
CalcId = NewType("CalcId", str)
NodeId = NewType("NodeId", str)
Digest = NewType("Digest", str)
Loc = NewType("Loc", Path)
IndexKey = Union[str, Sequence[str]]
Resources = Mapping[Loc, Digest]


@attr.s(auto_attribs=True)
class Index:
    data: str
    digest: Digest = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.digest = digest_str(self.data)

    def __iter__(self):
        items = json.loads(self.data)
        yield from self.items

    @staticmethod
    def from_items(items):
        data = unique_json(items)
        return Index(data)


class Storage(Protocol):
    def can_restore(self, digest: Digest) -> bool:
        """
        Does the storage have a resource with the given digest?
        """
        ...

    def restore(self, digest: Digest, loc: Loc):
        """
        Restore a resource with the given digest to the given location.
        """
        ...

    def store(self, loc: Loc) -> Digest:
        """
        Store the resource at given location and return its digest.
        """
        ...

    def read_bytes(self, digest: Digest) -> bytes:
        ...

    def write_bytes(self, data: bytes) -> Digest:
        ...


@attr.s(auto_attribs=True)
class Op:
    op_type: str
    options: JsonDict
    definition: JsonDict = attr.ib(init=False)
    op_id: OpId = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.definition = dict(op_type=self.op_type, options=self.options)
        self.op_id = OpId(unique_json_digest(self.definition))


@attr.s(auto_attribs=True)
class Node:
    op: Op
    inputs: Mapping[Loc, "Defn"]
    outputs: Sequence["Loc"] = attr.ib(converter=tuple)
    index_defn: "Defn"
    node_id: NodeId = attr.ib(init=False)

    def __attrs_post_init__(self):
        id_obj = dict(
            op_id=self.op.op_id,
            inputs={
                loc: dict(node_id=defn.node.node_id, loc=defn.loc)
                for loc, defn in self.inputs.items()
            },
            outputs=self.outputs,
            index_defn=dict(
                node_id=self.index_defn.node.node_id, loc=self.index_defn.loc
            ),
        )
        self.node_id = NodeId(unique_json_digest(id_obj))

    def get_defn(self, loc: Loc) -> Defn:
        return Defn(self, loc)


@attr.s(auto_attribs=True)
class Defn:
    node: Node
    loc: Loc

    def get_parents(self) -> AbstractSet["Defn"]:
        return set(self.node.inputs.values())


SINGLE_KEY = ":single:"
SINGLE_DEFN = NotImplemented

@attr.s(auto_attribs=True)
class DefnResult:
    defn: Defn
    index: Index
    key: IndexKey
    digest: Digest
    explicit: bool
    time: datetime.datetime = attr.ib(factory=datetime.datetime.utcnow)


@attr.s(auto_attribs=True)
class Calc:
    op: Op
    inputs: Mapping[Loc, Digest]
    calc_id: CalcId = attr.ib(init=False)

    def __attrs_post_init__(self):
        id_obj = dict(op_id=self.op.op_id, inputs=self.inputs)
        self.node_id = CalcId(unique_json_digest(id_obj))


@attr.s(auto_attribs=True)
class Run:
    calc: Calc
    start_time: datetime.datetime
    end_time: datetime.datetime
    results: Resources
    run_id: str = attr.ib(factory=get_uuid_string)


class NotFoundException(Exception):
    pass


class ConflictException(Exception):
    pass


class Log(Protocol):
    def save_run(self, run: Run):
        """
        Save a Run with dependencies.

        Save:
            * The Op
            * The Calc
            * The Run itself (including the results)
        """
        ...

    def save_node(self, node: Node):
        """
        Save a Node.

        Save:
            * The Node itself
            * The Op
            * Connections to upstream nodes
            * Defns
        """
        ...

    def save_defn_result(self, defn_result: DefnResult):
        """
        Note the Defn as (one possible) provenance of the corresponding digest.
        """
        ...

    def save_index(self, index: Index):
        """
        Save the concrete index.
        """
        ...

    def get_index(self, node: Node) -> Index:
        ...

    def get_calc(self, node: Node, key: IndexKey) -> Calc:
        """
        Get a Calc for the given Node at the given key.
        """
        ...

    def get_calc_result(self, calc: Calc, loc: Loc) -> Digest:
        """
        Get the digest that corresponds to the given Calc / Loc.

        Raises:
            NotFoundException if log has no result.
            ConflictException if log has multiple conflicting results.
        """
        ...

    def get_defn_result(self, defn: Defn, key: IndexKey) -> Digest:
        """
        Get the digest that corresponds to the given Defn / IndexKey.

        Raises:
            NotFoundException if log has no result.
            ConflictException if log has multiple conflicting results.
        """
        ...
