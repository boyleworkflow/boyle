from typing import Iterable, Union
import subprocess
import attr
import boyleworkflow.core
from boyleworkflow.core import Loc, Comp


def shell(cmd: str, inputs: Iterable[Comp], out: Union[Iterable[str], str]):
    if isinstance(out, str):
        out_list = [out]
    else:
        out_list = list(out)

    out_locs = list(map(Loc, out))

    op = boyleworkflow.core.Op(cmd=cmd, shell=True)

    comps = {
        out_loc: boyleworkflow.core.Comp(
            op=op,
            parents=tuple(inputs),
            loc=out_loc,
        )
        for out_loc in out_locs
    }

    if isinstance(out, str):
        assert len(comps) == 1, len(comps)
        comp, = comps.values()
        return comp
    else:
        return comps
