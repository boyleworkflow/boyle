# default arguments
# parameters and files
# prefer strings for paths
# rename inputs
# prefer function to get signature autocomplete / hints


def whatever(data, maxiter=5, script="scripts/whatever.py"):
    return Defn(
        inp={
            "data.pkl": data,
            "script.py": script,
        },
        param={"maxiter": maxiter},
        out="result.pkl",
        cmd="python script.py -o result.pkl --maxiter {maxiter} data.pkl",
    )


model = "models/model17.stan"

compiled = Defn(
    inp={
        "model.stan": model,
        "compile.py": "scripts/compile_stan.py",
    },
    out="compiled.pkl",
    cmd="python compile.py -o compiled.pkl",
)

# This one nedds to run the wrapped function later...it is not only data.


@task
def whatever2(data, maxiter=5):
    return Defn(
        inp={
            "data.pkl": data,
            "script.py": "scripts/whatever.py",
        },
        out="result.pkl",
        cmd=f"python script.py -o result.pkl --maxiter {maxiter} data.pkl",
    )


# This one is just data!
# It does depend on the "magic" that the function signature is analyzed
# so that maxiter can be identified as a parameter to be made available
# to the formatting of the cmd


@task
def whatever2(data, maxiter=5):
    return Defn(
        inp={
            "data.pkl": data,
            "script.py": "scripts/whatever.py",
        },
        out="result.pkl",
        cmd="python script.py -o result.pkl --maxiter {maxiter} data.pkl",
    )


@task
def warp_like(template, src):
    return Defn(
        inp={
            "template.tif": template,
            "src.tif": src,
        },
        out="dst.tif",
        cmd="rio warp --like template.tif -o dst.tif src.tif",
    )
