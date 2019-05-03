import os
import shutil
import logging

import attr

from boyleworkflow.core import PathLike, Digest, digest_file

logger = logging.getLogger(__name__)

@attr.s(auto_attribs=True)
class Storage:
    storage_dir: PathLike

    def __attrs_post_init__(self):
        os.makedirs(self.storage_dir, exist_ok=True)

    def _get_store_path(self, digest: Digest) -> PathLike:
        return os.path.join(self.storage_dir, digest)

    def can_restore(self, digest: Digest) -> bool:
        return os.path.exists(self._get_store_path(digest))

    def restore(self, digest: Digest, dst_path: PathLike):
        src_path = self._get_store_path(digest)
        logger.debug(f'Restoring {digest} to {dst_path}')
        shutil.copy2(src_path, dst_path)

    def store(self, src_path: PathLike) -> Digest:
        logger.debug(f'Storing {src_path}')
        digest = digest_file(src_path)
        dst_path = self._get_store_path(digest)
        shutil.copy2(src_path, dst_path)
        return digest
