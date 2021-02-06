import pytest
from boyleworkflow.loc import Name
from pathlib import Path
import hashlib
from typing import Mapping, Union
from boyleworkflow.storage import Storage, describe
from boyleworkflow.tree import Tree

FileOrDirSpec = Union[str, Mapping[str, "FileOrDirSpec"]]

ENCODING = "utf-8"
SOME_TEXT = "some text\n"
SOME_TEXT_SHA256 = "a23e5fdcd7b276bdd81aa1a0b7b963101863dd3f61ff57935f8c5ba462681ea6"


def write_str(s: str, path: Path):
    with open(path, "w", encoding=ENCODING) as f:
        f.write(s)


def read_str(path: Path) -> str:
    with open(path, "r", encoding=ENCODING) as f:
        return f.read()


def create_spec_tree(value: FileOrDirSpec) -> Tree:
    if isinstance(value, str):
        return Tree({}, {"sha256": value})
    else:
        return Tree({Name(k): create_spec_tree(v) for k, v in value.items()})


def get_hexdigest(data: bytes) -> str:
    m = hashlib.sha256()
    m.update(data)
    return m.hexdigest()


def test_describe_file(tmp_path: Path):
    file_path = tmp_path / "filename.txt"
    write_str(SOME_TEXT, file_path)

    expected_spec = create_spec_tree(SOME_TEXT_SHA256)
    spec = describe(file_path)
    assert spec == expected_spec


def test_describe_nested_dir(tmp_path: Path):
    subdir_1 = tmp_path / "subdir1"
    subdir_2 = subdir_1 / "subdir2"
    file_path = subdir_2 / "filename.txt"
    subdir_2.mkdir(parents=True)
    write_str(SOME_TEXT, file_path)

    expected_spec = create_spec_tree(
        {"subdir1": {"subdir2": {"filename.txt": SOME_TEXT_SHA256}}}
    )
    spec = describe(tmp_path)
    assert spec == expected_spec


def test_torage_can_create_dir(tmp_path: Path):
    storage_root_path = tmp_path / "nonexistent_dir"
    assert not storage_root_path.exists()
    Storage(storage_root_path)
    assert storage_root_path.exists()


def test_storage_wont_move_into_empty_dir(tmp_path: Path):
    storage_root_path = tmp_path / "empty_dir"
    storage_root_path.mkdir()
    assert storage_root_path.exists()
    with pytest.raises(FileNotFoundError):
        Storage(storage_root_path)


def test_can_create_dir_and_then_reopen(tmp_path: Path):
    storage_root_path = tmp_path / "nonexistent_dir"
    assert not storage_root_path.exists()
    Storage(storage_root_path)
    assert storage_root_path.exists()
    Storage(storage_root_path)


def test_store_describes_file(tmp_path: Path):
    file_path = tmp_path / "filename.txt"
    write_str("some text", file_path)

    storage = Storage(tmp_path / "storage")
    assert storage.store(file_path) == describe(file_path)


def test_store_can_restore_file(tmp_path: Path):
    src_path = tmp_path / "filename.txt"
    write_str("anything", src_path)

    storage = Storage(tmp_path / "storage")
    spec = storage.store(src_path)

    restore_path = tmp_path / "other_path.txt"
    storage.restore(spec, restore_path)

    assert describe(src_path) == describe(restore_path)


def test_store_can_restore_nested_dir(tmp_path: Path):
    subdir_1 = tmp_path / "subdir1"
    subdir_2 = subdir_1 / "subdir2"
    file_path = subdir_2 / "filename.txt"
    subdir_2.mkdir(parents=True)
    write_str(SOME_TEXT, file_path)

    storage = Storage(tmp_path / "storage")
    spec = storage.store(subdir_1)

    restore_path = tmp_path / "restore_path"
    storage.restore(spec, restore_path)

    assert describe(subdir_1) == describe(restore_path)


def test_store_can_restore_dir_having_stored_only_files(tmp_path: Path):
    src_path = tmp_path / "src_file"
    write_str(SOME_TEXT, src_path)

    storage = Storage(tmp_path / "storage")
    storage.store(src_path)

    restore_tree = create_spec_tree({"this": {"works": {"OK": SOME_TEXT_SHA256}}})

    restore_path = tmp_path / "restore_path"
    storage.restore(restore_tree, restore_path)

    assert describe(restore_path) == restore_tree


def test_reports_dir_as_restorable_having_stored_only_files(tmp_path: Path):
    src_path = tmp_path / "src_file"
    write_str(SOME_TEXT, src_path)
    storage = Storage(tmp_path / "storage")

    nested_tree = create_spec_tree({"this": {"works": {"OK": SOME_TEXT_SHA256}}})
    assert not storage.can_restore(nested_tree)

    storage.store(src_path)
    assert storage.can_restore(nested_tree)
