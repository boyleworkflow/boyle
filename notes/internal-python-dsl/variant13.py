@task
class CompileStan:
    inp = dict(script="compile_stan.py", model="model.stan")
    out = "model.pkl"
    cmd = "python {inp.script} {inp.model} {out}"


@task
class RunStan:
    inp = dict(
        script="run_stan.py",
        compiled_model="model.pkl",
        params="params.json",
        data="data.pkl",
    )
    out = "result.pkl"
    cmd = "python {inp.script}"  # relative paths hard-coded into the script


@task
class GenerateSyntheticData:
    inp = "synthetic_data.py"
    out = "data.pkl"
    cmd = "python {inp}"


@task
class PrepRealData:
    inp = ["data", "data.py"]
    out = "data.pkl"
    cmd = "python {inp[1]} {out}"


model = Input()  # must be specified
params = Input("stan-params.json")  # optional input

data = GenerateSyntheticData()
# data = DataPrep()

compiled = CompileStan(model=model)
result = RunStan(compiled_model=model, params=params, data=data).out

