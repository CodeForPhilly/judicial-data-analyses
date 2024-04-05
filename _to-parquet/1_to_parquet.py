#%%
import polars as pl
import polars.selectors as cs
from pathlib import Path

data_paths = Path("../_data").glob("*.txt")
Path("out").mkdir(exist_ok=True)

for path in data_paths:
    print(path)

    if "Alias" in path.name:
        continue

    # note that I deleted an unescaped | in a text field
    # on line 1965440 of CPCMS_SentenceData
    df = pl.read_csv(path, separator="|", infer_schema_length=100_000_000_000)

    if "CaseData" in path.name:
        # hash defendant name and date of birth
        res = (
            df
            .with_columns(
                DefendantID=(pl.col("DefendantName") + "---" + pl.col("DefendantDOB")).hash(),
                DefendantDOB=pl.col("DefendantDOB").str.to_date().dt.truncate("1mo")
            )
            .select(cs.all().exclude("DefendantName"))
        )
    else:
        res = df
 
    res.write_parquet(f"out/{path.with_suffix('.parquet').name}")

# %%
