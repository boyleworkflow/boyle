"""usage: gpc make [-h|--help] <target> [<target>]

Make a target, i.e. run calculations that are necessary to generate the target
file. If the target file already exists in cache, simply copy it into working
directory.
"""
from docopt import docopt
from gpc.gpc import *
from gpc import spec_reader

def make(target):
    log = Log('log')
    storage = Storage('storage')
    graph = spec_reader.graph_from_spec('gpc.yaml')

    runner = Runner(log, storage, graph)
    runner.make(target)
    
    responsible_runs = log.get_provenance(digest_file(target))
    print('The file was produced by %i run(s):' % len(responsible_runs))
    for r in responsible_runs:
        print(r)

if __name__ == '__main__':
    args = docopt(__doc__)
    make(args['<target>'][0])
