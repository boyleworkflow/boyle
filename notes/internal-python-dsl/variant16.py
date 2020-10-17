compile_stan = Task(
    inp=dict(script="scripts/compile_stan.py", model="model.stan"),
    out="compiled.py",
    cmd="python {inp.script} {inp.model} {out}",
)

run_stan = Task(
    inp=dict(script="scripts/run_stan.py", model="model.pkl", data="data.pkl"),
    out="results.pkl",
    cmd="python {inp.script}",  # hard coded paths in the script
)

compiled = compile_stan(model="models/model17.stan")


def compile_and_run(model, data):
    compiled = compile_stan(model=model)
    result = run_stan(model=compiled, data=data)
    return result


models = glob(r"models/*.stan")
with each(models) as model:
    result = compile_and_run(model)

more_models = {
    "a": "other/dir/model.stan",
    "b": "other/dir/test.stan",
    "c": "third_dir/model.stan",
}

more_results = {k: compile_and_run(model=v) for k, v in more_models.items()}


resample = Task(
    inp="src.tif",
    out="dst.tif",
    cmd="run_thing -k {params.k} -z {params.z} {inp} {out}",
    params=["res", "method"],
)

src_rasters = Import(
    ["glw/glw_cattle.tif", "glw/glw_chickens.tif", "glw/glw_pigs.tif"]
)

k = Param("k", values=[1, 2, 4, 8, 16])
z = Param("z", values=[10, 100, 1000])

kz = Params(["k", "z"], values=[(1, 10)])


resampled_glw = resample(src_rasters)
