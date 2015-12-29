from gpc.gpc import *
from gpc import spec_reader

def main():
    log = Log('log')
    storage = Storage('storage')
    graph = spec_reader.graph_from_spec('simple.yaml')

    runner = Runner(log, storage, graph)
    runner.make('c')
    
    responsible_runs = log.get_provenance(digest_file('c'))
    print('The file was produced by %i run(s):' % len(responsible_runs))
    for r in responsible_runs:
        #print_run(r)
        print(r)

if __name__ == '__main__':
    main()
