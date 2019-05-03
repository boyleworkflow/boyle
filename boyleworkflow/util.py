import os
import stat


BITS = {
    'write': [stat.S_IWUSR, stat.S_IWGRP, stat.S_IWOTH],
    'read': [stat.S_IRUSR, stat.S_IRGRP, stat.S_IROTH],
    'execute': [stat.S_IXUSR, stat.S_IXGRP, stat.S_IXOTH],
}


def _enable(permissions, bit):
    return permissions | bit

def _disable(permissions, bit):
    return permissions & (~bit)

def set_file_permissions(path, read=None, write=None, execute=None):
    permissions = stat.S_IMODE(os.stat(path).st_mode)

    choices = dict(read=read, write=write, execute=execute)

    for k, v in choices.items():
        if v is None:
            continue

        operation = _enable if v else _disable

        for bit in BITS[k]:
            permissions = operation(permissions, bit)

    os.chmod(path, permissions)


def get_file_permissions(path):
    permissions = stat.S_IMODE(os.stat(path).st_mode)

    booleans = {}
    for k, bits in BITS.items():
        booleans[k] = tuple((permissions & bit) > 0 for bit in bits)

    return booleans
