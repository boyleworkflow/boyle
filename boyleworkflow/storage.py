from typing import NewType
import os
import shutil
import logging
from pathlib import Path
import attr

from boyleworkflow.util import set_file_permissions, digest_file
from boyleworkflow.core import Digest

logger = logging.getLogger(__name__)


class RestoreError(Exception):
    pass


@attr.s(auto_attribs=True)
class Storage:
    storage_dir: Path = attr.ib(converter=Path)

    def __attrs_post_init__(self):
        os.makedirs(self.storage_dir, exist_ok=True)
        os.makedirs(os.path.join(self.storage_dir, "meta"), exist_ok=True)

    def _get_store_path(self, digest: Digest) -> Path:
        return self.storage_dir / digest

    def _get_meta_path(self, digest: Digest) -> Path:
        return self.storage_dir / "meta" / digest

    def _appears_unchanged(self, digest: Digest):
        src_path = self._get_store_path(digest)
        meta_path = self._get_meta_path(digest)

        src_mtime = os.path.getmtime(src_path)
        meta_mtime = os.path.getmtime(meta_path)

        if meta_mtime != src_mtime:
            return False

        return True

    def _set_meta(self, digest: Digest):
        src_path = self._get_store_path(digest)
        meta_path = self._get_meta_path(digest)

        try:
            os.remove(meta_path)
        except FileNotFoundError:
            pass

        with open(meta_path, "w"):
            pass

        shutil.copystat(src_path, meta_path)

    def can_restore(self, digest: Digest) -> bool:
        if not os.path.exists(self._get_store_path(digest)):
            return False

        if not self._appears_unchanged(digest):
            return False

        return True

    def restore(self, digest: Digest, dst_path: Path):
        logger.debug(f"Restoring {digest} to {dst_path}")

        if not self.can_restore(digest):
            raise RestoreError(f"error restoring {digest}")

        src_path = self._get_store_path(digest)
        set_file_permissions(src_path, write=False, read=True)

        os.link(src_path, dst_path)

    def store(self, src_path: Path) -> Digest:
        logger.debug(f"Storing {src_path}")
        digest = Digest(digest_file(src_path))
        if self.can_restore(digest):
            return digest

        dst_path = self._get_store_path(digest)
        try:
            # It is possible that a file with the given name exists,
            # although the file cannot be restored. This happens if
            # can_restore() returns False due to a suspected modification.
            # So try to remove the file before linking in the new one.
            os.remove(dst_path)
        except FileNotFoundError:
            pass

        set_file_permissions(src_path, write=False, read=True)
        os.link(src_path, dst_path)
        self._set_meta(digest)
        return digest
