import os
import attr
import shutil
import logging

logger = logging.getLogger(__name__)

@attr.s
class Storage:

    storage_dir = attr.ib()

    def __attrs_post_init__(self):
        os.makedirs(self.storage_dir, exist_ok=True)

    def _get_store_path(self, resource):
        return os.path.join(self.storage_dir, resource.digest)

    def can_restore(self, resource, base_dir):
        return os.path.exists(self._get_store_path(resource))

    def restore(self, resource, base_dir):
        src_path = self._get_store_path(resource)
        dst_path = os.path.join(base_dir, resource.loc)
        logger.debug(f'Restoring {resource} to {dst_path}')
        shutil.copy2(src_path, dst_path)

    def store(self, resource, base_dir):
        src_path = os.path.join(base_dir, resource.loc)
        logger.debug(f'Storing {resource} from {src_path}')
        dst_path = self._get_store_path(resource)
        shutil.copy2(src_path, dst_path)
