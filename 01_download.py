# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3.7.12 ('13f')
#     language: python
#     name: python3
# ---

# %%
# import requests
import requests_cache

session = requests_cache.CachedSession(".data/interim/.demo_cache")
import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm


import logging
import urllib.request
import shutil
import backoff
import ssl, requests, os

import numpy as np
import humanfriendly


logging.basicConfig(stream=os.sys.stdout, level=logging.INFO)
logger = logging.getLogger('nb')


# %%

# %%
# index files recursively

files = []
folders = ["https://b.scsi.to/"]

while len(folders):
    folder = folders.pop()
    print("folder:", folder)
    r = session.get(folder)
    r.raise_for_status()

    df = pd.read_html(r.content)[0].iloc[1:]
    df["url"] = r.request.url + df["Name"]
    df["rel_path"] = df["url"].str.split("https://b.scsi.to/").apply(lambda s: s[1])
    df

    folders += df[df.Name.str.endswith("/")].url.tolist()
    # print(folders)

    files.append(df[~df.Name.str.endswith("/")])

print(folders)
# join and shuffle, so we can run some in parrelal in needed
files = pd.concat(files).sample(frac=1)
files["size"] = files.Size.apply(humanfriendly.parse_size)
files


# %%
class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        return self.update(b * bsize - self.n)  # also sets self.n = b * bsize


# %%
download_folder = Path("/media/wassname/WD14A/data/options/b.scsi.to")

# %%


# we need headers to avoid 403 error
opener = urllib.request.build_opener()
opener.addheaders = [("User-agent", "Mozilla/5.0")]
urllib.request.install_opener(opener)

# check size
def check_size(row, dest, rm=True):
    size_correct = humanfriendly.format_size(row["size"])
    size_downloaded = humanfriendly.format_size(dest.stat().st_size)
    msg = f"size is {size_downloaded} but should be {size_correct} {dest_temp}"
    try:
        np.testing.assert_approx_equal(
            dest.stat().st_size, row["size"], significant=2, err_msg=msg
        )
        return True
    except AssertionError as e:
        print(e)
        print(f"rm {dest}")
        if rm:
            dest.unlink()  # delete
        return False




"""
track errors received

ssl.SSLError: [SSL: KRB5_S_TKT_NYV] unexpected eof while reading (_ssl.c:2570)
"""

logging.getLogger('backoff').setLevel(logging.INFO)


@backoff.on_exception(
    backoff.expo,
    (
        requests.exceptions.RequestException,
        ssl.SSLError,
        urllib.error.URLError,
    ),
    max_tries=8,
)
def download(url, dest):
    """download with progbar"""
    with TqdmUpTo(
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        mininterval=1,
        desc=str(dest)[-50:],
    ) as t:
        urllib.request.urlretrieve(url, dest, reporthook=t.update_to)


for name, row in tqdm(files.iterrows(), total=len(files), desc="files"):
    dest = download_folder / row.rel_path
    dest.parent.mkdir(exist_ok=True, parents=True)
    dest_temp = Path(str(dest) + ".unfinished")
    if not dest.is_file():
        download(row.url, dest_temp)

        if check_size(row, dest_temp, rm=True):
            shutil.move(dest_temp, dest)

    else:
        check_size(row, dest, rm=False)


# %%

# %%

# %%
# np.testing.assert_almost_equal(dest.stat().st_size, row['size'])


# %%

# %%

# %%
