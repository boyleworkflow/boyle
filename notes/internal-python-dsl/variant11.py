compile_stan = Task(
    inp=dict(script="compile_stan.py", model="model.stan"),
    out="model.pkl",
    cmd="python {inp.script} {inp.model} {out}",
)

run_stan = Task(
    inp=dict(
        script="run_stan.py",
        compiled_model="model.pkl",
        params="params.json",
        data="data.pkl",
    ),
    out="result.pkl",
    cmd="python {inp.script}",  # relative paths hard-coded into the script
)

generate_synthetic_data = Task(
    inp="synthetic_data.py", out="data.pkl", cmd="python {inp}"
)

load_real_data = Task(
    inp=["data", "data.py"], out="data.pkl", cmd="python {inp[1]} {out}"
)

result = run_stan(model="stan_stuff/models/model")

model = Input()  # must be specified
params = Input("stan-params.json")  # optional input

data = generate_synthetic_data()
# data = load_real_data()


compiled = compile_stan(model=model)
result = run_stan(compiled_model=model, params=params, data=data)
