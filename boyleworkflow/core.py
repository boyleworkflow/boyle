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
    Iterable,
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
TreeId = NewType("TreeId", str)
BlobId = NewType("BlobId", str)
Loc = NewType("Loc", Path)
Name = NewType("Name", str)

Resource = Union["Tree", BlobId]

class TreeConflictException(ValueError):
    pass

@attr.s(auto_attribs=True)
class Tree:
    items: Mapping[Name, Resource]
    tree_id: TreeId = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.tree_id = TreeId(
            unique_json_digest(dict(type="tree", items=self.items))
        )

    def merge(self, other: Tree) -> Tree:
        raise NotImplementedError()

    @staticmethod
    def from_resources(resources: Mapping[Loc, Resource]) -> Tree:
        raise NotImplementedError()



def merge_trees(trees: Iterable[Tree]) -> Tree:
    raise NotImplementedError()


@attr.s(auto_attribs=True)
class Op:
    op_type: str
    depth: int
    options: JsonDict
    definition: JsonDict = attr.ib(init=False, repr=False)
    op_id: OpId = attr.ib(init=False, repr=False)

    def __attrs_post_init__(self):
        self.definition = dict(
            op_type=self.op_type, options=self.options, depth=self.depth
        )
        self.op_id = OpId(unique_json_digest(self.definition))


@attr.s(auto_attribs=True)
class Node:
    op: Op
    inputs: Mapping[Loc, "Defn"]
    outputs: Collection[Loc]
    depth: int
    node_id: NodeId = attr.ib(init=False, repr=False)

    def __attrs_post_init__(self):
        id_obj = dict(
            op_id=self.op.op_id,
            inputs=[inp_node.node_id for inp_node in self.inputs],
            outputs=list(sorted(self.outputs)),
            depth=self.depth,
        )
        self.node_id = NodeId(unique_json_digest(id_obj))


@attr.s(auto_attribs=True)
class Defn:
    node: Node
    loc: Loc


@attr.s(auto_attribs=True)
class Calc:
    op: Op
    input_tree: Tree


@attr.s(auto_attribs=True)
class Run:
    calc: Calc
    output_tree: Tree
    start_time: datetime.datetime
    end_time: datetime.datetime
    run_id: str = attr.ib(factory=get_uuid_string)


@attr.s(auto_attribs=True)
class NodeResult:
    node: Node
    output_tree: Tree
    explicit: bool
    time: datetime.datetime = attr.ib(factory=datetime.datetime.utcnow)


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
            * The Run itself (including the result Tree)
        """
        ...

    def save_node_result(self, node_result: NodeResult):
        """
        Save a NodeResult with dependencies.

        This notes the Node as (one possible) provenance of a Tree.

        Save:
            * The Node and connections to parent Nodes (but not upstream nodes)
            * The NodeResult itself
        """
        ...

    def get_calc(self, node: Node) -> Calc:
        """
        Get the Calc corresponding to the node.

        Raises:
            NotFoundException if log has no result.
            ConflictException if log has multiple conflicting results.
        """

        ...

    def get_calc_result(self, calc: Calc, outputs: Iterable[Loc]) -> Tree:
        """
        Get the tree representing output from a Calc restricted to some Locs.

        Raises:
            NotFoundException if log has no result.
            ConflictException if log has multiple conflicting results.
        """
        ...

    def get_node_result(self, node: Node) -> Tree:
        """
        Get the tree representing output from the given calc.

        Raises:
            NotFoundException if log has no result.
            ConflictException if log has multiple conflicting results.
        """
        ...


class Storage(Protocol):
    def can_restore(self, resource: Resource) -> bool:
        """
        Does the storage have data to restore the given Resource?
        """
        ...

    def restore(self, tree: Tree, loc: Loc):
        """
        Restore a Tree to the given Loc.
        """
        ...

    def store(self, loc: Loc) -> Resource:
        """
        Store the Resource at given location and return a description.
        """
        ...
