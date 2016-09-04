class Definition:
    # out: one Resource
    # inp: zero or more Resource
    # do: zero or more Operation

class Resource:
    def digest(): pass
    def save(): pass # make sure it can be restored
    def release(): pass ???
    def restore(digest): pass # place in working dir
    def represent(digest): pass

    name = None
    is_variable = False



class File:
    def restore(digest):
        # assume that os.getcwd() is where the operation happens
        path = os.path.join(os.getcwd(), self.rel_path)
        self.file_storage.restore(digest, path)
    def represent():
        # assume that os.getcwd() is end user's working dir
        self.restore()


class Operation:
    pass


def define(out):


    return Definition(name=out.name)


# Examples of classes that would implement Resource and Operation interfaces

# Resource:
#     File
#     Files
#     Directory
#     Value
#     YAMLValue
#     Values
#     Http
#     HDFS
#     ...

# Operation:
#     Shell
#     Python