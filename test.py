import tempfile

import attr

import boyleworkflow as boyle

with tempfile.TemporaryDirectory() as td:

    storage = boyle.Storage(td)
    log = boyle.Log("log.sqlite")

    a = boyle.shell("echo hello > a", inputs=(), out="a")
    # b = boyle.shell('echo hello > a')

    # print(a)
    #
    b = boyle.shell("echo world > b", inputs=(), out="b")

    c = boyle.shell("cat a b > c && echo test", inputs=(a, b), out="c")

    c_fail = boyle.shell("cat x y > c", inputs=(a, b), out="c")

    results = boyle.make([c], log, storage)

    for comp, digest in results.items():
        print(comp.comp_id, digest)

    boyle.make([c_fail], log, storage)
