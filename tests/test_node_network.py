from __future__ import annotations
from dataclasses import dataclass
from typing import FrozenSet
import pytest
from boyleworkflow.scheduling import get_nodes_and_ancestors, get_root_nodes


@dataclass(frozen=True)
class Node:
    parents: FrozenSet[Node]

    @classmethod
    def from_parents(cls, parents):
        return cls(frozenset(parents))


@pytest.fixture
def root1():
    return Node.from_parents([])


@pytest.fixture
def root2():
    return Node.from_parents([])


@pytest.fixture
def mid1(root1):
    return Node.from_parents({root1})


@pytest.fixture
def mid2(root1, root2):
    return Node.from_parents({root1, root2})


@pytest.fixture
def bottom1(mid1):
    return Node.from_parents({mid1})


@pytest.fixture
def bottom2(mid2):
    return Node.from_parents({mid2})


def test_root_ancestors(root1):
    assert get_nodes_and_ancestors([root1]) == {root1}


def test_bottom_ancestors(root1, root2, mid1, mid2, bottom1, bottom2):
    bottom1_expected = {bottom1, mid1, root1}
    bottom2_expected = {bottom2, mid2, root1, root2}
    bottom_union = bottom1_expected | bottom2_expected

    assert get_nodes_and_ancestors([bottom1]) == bottom1_expected
    assert get_nodes_and_ancestors([bottom2]) == bottom2_expected
    assert get_nodes_and_ancestors([bottom1, bottom2]) == bottom_union


def test_root_roots(root1):
    assert get_root_nodes(root1) == {root1}


def test_bottom_roots(root1, root2, bottom1, bottom2):
    assert get_root_nodes(bottom1) == {root1}
    assert get_root_nodes(bottom2) == {root1, root2}
