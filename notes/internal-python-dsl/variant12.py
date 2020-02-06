@task
class Compilation:
    inp = dict(script="compile_stan.py", model="model.stan")
    out = "model.pkl"
    cmd = "python {inp.script} {inp.model} {out}"


@task
class Execution:
    inp = dict(
        script="run_stan.py",
        compiled_model="model.pkl",
        params="params.json",
        data="data.pkl",
    )
    out = "result.pkl"
    cmd = "python {inp.script}"  # relative paths hard-coded into the script


@task
class SyntheticDataGen:
    inp = "synthetic_data.py"
    out = "data.pkl"
    cmd = "python {inp}"


@task
class DataPrep:
    inp = ["data", "data.py"]
    out = "data.pkl"
    cmd = "python {inp[1]} {out}"


result = run_stan(model="stan_stuff/models/model")

model = Input()  # must be specified
params = Input("stan-params.json")  # optional input

data = SyntheticDataGen().out
# data = DataPrep().out


compiled = Compilation(model=model).out
result = Execution(compiled_model=model, params=params, data=data).out
