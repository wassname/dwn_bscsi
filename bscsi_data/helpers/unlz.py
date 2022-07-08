import subprocess
from pathlib import Path
from ..config import path_interim, path_raw
import functools


def unlz_unbound(f, path_interim, path_raw, do=True):
    """
    unzip a lz file from raw to interim if needed

    returns unlzipped path

    uses linux program lzip which must be installed
    """

    f_dest = path_interim / f.relative_to(path_raw)
    f_dest = f_dest.parent / f_dest.stem
    assert str(f).endswith(".lz"), "must be a lzip file"

    if do and (not f_dest.exists()):
        f_dest.parent.mkdir(exist_ok=True, parents=True)
        subprocess.check_output(["lzip", "--keep", "-d", str(f), "-o", str(f_dest)])

    return f_dest


unlz = functools.partial(unlz_unbound, path_raw=path_raw, path_interim=path_interim)
