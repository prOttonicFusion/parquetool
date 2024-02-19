import pandas as pd


def parquetToDataFrame(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df
