@task
@out("model.pkl")
def compile_stan(script="compile_stan.py", model="model.stan"):
    return "python {script} {model} {out}"


@task
@out("result.pkl")
def run_stan(
    script="run_stan.py",
    compiled_model="model.pkl",
    params="params.json",
    data="data.pkl",
):
    return "python {script}"

@task
@out("data.pkl")
def generate_synthetic_data(script="synthetic_data.py"):
    return "python {script}"

@task
@out("data.pkl")
def prep_real_data(data_dir="data", script="data.py"):
    return "python {data_dir} {out}"


model = Input()  # must be specified
params = Input("stan-params.json")  # optional input

data = generate_synthetic_data()
# data = DataPrep()

compiled = compile_stan(model=model)
result = run_stan(compiled_model=model, params=params, data=data)

