class Context:
    user = ...
    def get_result(calculation, instrument, tmax): pass


def _run(calculation, output_instruments, storage):
    with Context() as context:
        for r in calculation.inputs:
            r.restore(storage, context)

        run_script = '''
            import boyle
            procedure = boyle.Procedure.from_json({})
            procedure()
            '''.format(calculation.procedure.to_json())
        subprocess.run('python -c "{}"'.format(run_script), context.workdir)

        results = {
            instrument: instrument.save(context, storage)
            for instrument in output_instruments
            }

        return results



# Resources should probably be orthogonal in most cases.
# But this is hard to guarantee since they are allowed to change things
# around them as they are loaded into a context. So let's decide
# that they are ordered.

# context/workdir
# context/resources/af388c.../resource.json
# context/resources/af388c.../data/...

# script = '''
#     import boyle
#     inp = boyle.load_resources()
#     {{ user_script }}
#     {% for vi in value_instruments %}
#     boyle.save_value({{ vi.name }}, {{ vi.name }})
#     {% endfor %}
#     '''

# octave_script = '''
#     pkg load boyle
#     {% for vr in value_resources_in %}
#     inp.{{ vr.instrument.name }} = loadvalue('{{ vr.digest }}')
#     {% endfor %}
#     {{ user_octave_script }}
#     {% for instr in out_instruments %}
#     savevalue({% instr.name %})
#     {% endfor %}
#     '''

def ensure_restorable(requested_defs, storage):

    run_needed = {}
    while True:
        for calc, instruments in run_needed.items():
            _run(calc, mommy)
        try:
            return _resolve(requested_defs, log, storage)
        except RunRequired as e:
            run_needed = e.calculations


def _resolve(requested_defs, log, storage):
    """
    Tries to resolve the Resources defined by Definitions defs.

    Args:
        requested_defs: An iterable of requested Definitions.
        log: A Log to read from and write in.
        storage: A Storage for resources.

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
        log.get_result(
            calculation=get_calculation(definition),
            instrument=definition.instrument,
            tmax=time,
            )

    requested_defs = set(requested_defs)
    defs = get_upstream_sorted(requested_defs)

    run_needed = defaultdict(set)
    for d in defs:
        if d.inputs.intersection(set(run_needed)):
            raise RunRequired(run_needed)

        calc = get_calculation(d)
        try:
            resource = get_result(d)
        except NotFound:
            run_needed[calc].add(d.instrument)
            continue

        if d in requested_defs and not resource.can_restore_from(storage):
            run_needed[calc].add(d.instrument)

    assert not run_needed
    return {d: get_result(d) for d in requested_defs}
