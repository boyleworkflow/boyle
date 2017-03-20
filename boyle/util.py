def define(inp=None, out=None, do=None):
    # TODO: Plenty more input validation, since this is the most central
    # part of the API from the end user's perspective
    if out is None:
        raise ValueError('the definition must define something')
    if hasattr(do, 'run'):
        do = (do,)
    inp = () if inp is None else tuple(inp)
    do = () if do is None else tuple(do)
    if not all(callable(item.run) for item in do):
        raise ValueError('all the operations must be callable')

    # TODO: Which sorts of inputs could out be, really? It seems to make sense
    # that it can be compositions of lists/tuples and dicts, where all leaf
    # nodes are ResourceHandlers.
    if not isinstance(out, collections.Sequence):
        out = (out,)

    defs = tuple(
        Definition(inputs=inp, procedure=do, instrument=out_item)
        for out_item in out)

    if len(defs) == 1:
        return defs[0]
    else:
        return defs
