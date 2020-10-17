# Let's have implicit and optional renaming!


def warp_like(template, src):
    # The default behavior here could be to rename the inputs to
    # "template.{ext}" and "src.{ext}", i.e., keeping just the extension
    # of the input.
    out = "dst.tif"
    cmd = "rio warp -o {out} --like {template} {src}"
    return Defn.auto()


def warp_like(template, src):
    rename = False
    out = "dst.tif"
    cmd = "rio warp --like {template} -o {out} {src}"
    return Defn.auto()


def warp_like(template, src):
    for inp in [template, src]:
        if not inp.path.endswith(".tif"):
            raise ValueError("I only accept tif files")

    rename = dict(template="hard_coded_template_name.tif", src="src.tif")

    out = "hard_coded_output_name.tif"
    cmd = "inconvenient_command {src}"
    return Defn.auto()


def warp_to_resolution(src, resolution=3.14):
    out = "dst.tif"
    cmd = "rio warp {src} {out} --res {resolution}"
    return Defn.auto()


clc = File("clc.tif")
glw = File("glw/chickens.tif")

glw_warped = warp_like(clc, glw)

glw_warped_shrunk = warp_to_resolution(glw, 10)
