def compile_stan(script="scripts/compile_stan.py", model="model.stan"):
    out(compiled="compiled.pkl")
    cmd("python {inp.script} {inp.model} {out.compiled}")


def run_stan(script="scripts/run_stan.py", model="model.pkl", data="data.pkl"):
    out(results="results.pkl")
    cmd("python {inp.script}")  # hard coded paths in the script


compiled = compile_stan(model="models/model17.stan")


def compile_and_run(model, data):
    compiled = compile_stan(model=model)
    result = run_stan(model=compiled, data=data)
    return result


models = glob(r"models/*.stan")
model_data = "indata/model_data.pkl"
results = compile_and_run(model, model_data)

more_models = {
    "a": "other/dir/model.stan",
    "b": "other/dir/test.stan",
    "c": "third_dir/model.stan",
}

more_results = compile_and_run(more_models, model_data)


class File:
    def __class_getitem__(cls, item):
        cls.__filename__ = item


class Param:
    pass


@task
def resample(src: File, k: Param, z: Param):
    cmd("run_thing --log {out.log} -k {inp.k} -z {inp.z} {inp.src} {out.dst}")
    return dict(dst=File["out.tif"], log=File["log.txt"])


a = resample(1, 2, 3)

k_vals = [1, 2, 4, 8, 16]
z_vals = [10, 100, 1000, 10000, 100000]

src_rasters = ["glw/glw_cattle.tif", "glw/glw_chickens.tif", "glw/glw_pigs.tif"]

with Each(src_rasters) as src:
    with Zip(k_vals, z_vals) as (k, z):
        resampled_glw = resample(src, k, z)


# or implicitly broadcasting src over the arrays (which have matching length)
with Each(src_rasters) as src:
    resampled_glw = resample(src, k_vals, z_vals)
