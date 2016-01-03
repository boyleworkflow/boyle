import os
import shutil

class Storage(object):
    """docstring for Storage"""
    def __init__(self, path):
        """
        Open a storage.
        """
        super(Storage, self).__init__()
        self.path = os.path.abspath(path)
        if not os.path.isdir(self.path):
            raise GenericError('No storage at {}'.format(self.path))

    @staticmethod
    def create(path):
        """
        Create a storage.
        """
        path = os.path.abspath(path)
        if os.path.exists(path):
            raise GenericError('Path {} already exists'.format(path))
        os.makedirs(path)

    def has_file(self, digest):
        """Check if a file exists in the storage.

        Args:
            digest (str): A file digest.

        Returns:
            bool: A value indicating whether the file is available.

        """
        return os.path.exists(os.path.join(self.path, digest))


    def copy_to(self, digest, path):
        """Copy a file from the storage.

        Args:
            digest (str): A file digest.
            path (str): A path to put the file at.

        Raises:
            KeyError: If the file does not exist in the storage.
        """
        src_path = os.path.join(self.path, digest)
        if not os.path.exists(src_path):
            raise KeyError
        shutil.copy2(src_path, path)


    def save(self, path):
        """Save a file in the storage.

        Note:
            The file is moved to the storage, not copied.

        Args:
            path (str): A path where the file is located.

        Returns:
            str: The digest of the file.
        """
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        digest = digest_file(path)
        dst_path = os.path.join(self.path, digest)
        shutil.move(path, dst_path)

        return digest
