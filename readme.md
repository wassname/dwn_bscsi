download, preproc, and load optionmetris data from http://b.scsi.to/

- get the `https://b.scsi.to/optm_lz` and put in data/raw. Download with `notebooks/000_000_download.py` it will take days
- run `notebooks/000_001_preproc_sas7bdat2parq.py` it will take a 5 days or so and will make the options into parquets, compressed, and chunked by security
