
class Context:
    user = ...

def ensure_restorable(requested_defs, ctx):

    run_needed = set()
    while True:
        for calc in run_needed:
            run(calc, ctx)
        try:
            return _resolve(requested_defs, ctx)
        except RunRequired as e:
            run_needed = e.calculations


def _resolve(requested_defs, ctx):
    """
    Tries to resolve the Resources defined by Definitions defs.

    Args:
        requested_defs: An iterable of requested Definitions.
        ctx: The "home" Context to start the work from.

    Returns:
        A dictionary of {definition: resource} pairs.

    Raises:
        RunRequired, if additional calculations must be run before
        the defs can be resolved.
    """

    # Mathematically this is seen as a function mapping
    # (Time, User, Definition) -> Resource | Requirement[]
    # So the first thing to note is
    # time = now   
    # user = ctx.user

    def get_calculation(definition):
        return Calculation(
            definition.procedure,
            [get_result(inp) for inp in definition.inputs])

    def get_result(definition):
        ctx.get_result(
            calculation=get_calculation(definition),
            instrument=definition.instrument,
            tmax=time,
            )

    requested_defs = set(requested_defs)
    defs = get_upstream_sorted(requested_defs)

    run_needed = set()
    for d in defs:
        if run_needed.intersection(d.inputs):
            raise RunRequired(run_needed)

        calc = get_calculation(d)
        try:
            resource = get_result(d)
        except NotFound:
            run_needed.add(calc)
            continue

        if minimum_progress[d] in requested_defs and not ctx.can_restore(d):
            run_needed.add(d)

    return {d: get_result(d) for d in requested_defs}
