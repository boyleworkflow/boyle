from typing import Mapping, Iterable, Union
import subprocess
import attr
import boyle.core
from boyle.core import Loc, Comp


def shell(cmd: str, inputs: Mapping[str, Comp], out: Union[Iterable[str], str]):
    if isinstance(out, str):
        out_list = [out]
    else:
        out_list = list(out)

    out_locs = list(map(Loc, out))

    op = boyle.core.Op(cmd=cmd, shell=True)

    comps = {
        out_loc: boyle.core.Comp(
            op=op,
            inputs={Loc(loc): comp for loc, comp in inputs.items()},
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
