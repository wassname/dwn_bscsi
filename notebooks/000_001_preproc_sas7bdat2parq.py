"""
Take a file likes opprcd1998.sas7bdat

- un-lzip it
- turn it into a parquet, which is partitioned by security, and compressed
- now you can read it without crashing your machine!

"""
from tqdm.contrib.concurrent import process_map

import pyreadstat
import dask.dataframe as dd
from dask.delayed import delayed
import dask.dataframe as ddf
import numpy as np
import pandas as pd
import gc
from dask.diagnostics import ProgressBar
from pathlib import Path
from src.data.helpers import unlz, path_raw, path_interim

max_workers = 2


def dask_sas_reader(filepath, chunksize):
    # Read metadata only of the SAS file in order to find out the number of rows
    _, meta = pyreadstat.read_sas7bdat(
        filepath, disable_datetime_conversion=False, metadataonly=True
    )

    # Helper function which reads a chunk of the SAS file
    def read_sas_chunk(offset):
        df, _ = pyreadstat.read_sas7bdat(
            filepath,
            disable_datetime_conversion=False,
            row_offset=offset,
            row_limit=chunksize,
        )
        df["date"] = pd.to_datetime(df["date"])
        df["exdate"] = pd.to_datetime(df["exdate"])
        df["last_date"] = pd.to_datetime(df["last_date"])
        #         df = df.set_index('optionid')
        return df

    # Parallelize reading of chunks using delayed() and combine these in a dask dataframe
    dfs = [delayed(read_sas_chunk)(x) for x in range(0, meta.number_rows, chunksize)]
    return dd.from_delayed(dfs)


def to_parq(f):
    f2 = unlz(f, do=False)
    f_dest = f2.parent / (f2.stem + ".parq")
    if not f_dest.exists():
        unlz(f, do=True)
        dd_df = dask_sas_reader(f2, chunksize=500000)
        print("to parq", f_dest, f2)
        with ProgressBar():
            dd_df.to_parquet(
                str(f_dest), write_index=False, partition_on=["secid"], engine="pyarrow"
            )
    else:
        print(f"{f} already exists")


if __name__ == "__main__":
    print(path_raw)
    raw_files = sorted((path_raw / "optm_lz/opprcd").glob("opprcd*.lz"))
    print(raw_files)
    # note this took me 5d on 2 threads
    r = process_map(to_parq, raw_files, max_workers=max_workers)
