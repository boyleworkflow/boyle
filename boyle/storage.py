@attr.s
class Storage:

    storage_dir = attr.ib()

    def _get_store_path(resource):
        return os.path.join(self.storage_dir, resource.digest)

    def can_restore(self, resource, base_dir):
        return os.path.exists(self._get_store_path(resource))

    def restore(self, resource, base_dir):
        src_path = _get_temp_store_path(resource)
        dst_path = os.path.join(base_dir, resource.uri)
        shutil.copy2(_get_temp_store_path(resource), dst_path)
