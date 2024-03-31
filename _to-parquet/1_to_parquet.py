#%%
import polars as pl
from pathlib import Path

data_paths = Path("../_data").glob("*.txt")
Path("out").mkdir(exist_ok=True)

for path in data_paths:
    if "CPCMS_SentenceData" in path.name:
        # note that for some reason, both polars and pandas CSV
        # readers believe that some lines in this file have 26 fields,
        # when they should have 25. I looked at one manually (1964815)
        # and couldn't see anything wrong with it.
        continue

    df = pl.read_csv(path, separator="|", infer_schema_length=100_000_000_000, **opts)

    df.write_parquet(f"out/{path.with_suffix('.parquet').name}")

