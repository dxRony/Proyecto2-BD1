import pandas as pd
from pathlib import Path
from utils.normalizers import dataframe_normalize_headers


def read_csv_file(
    file_path: str,
    sep: str = ",",
    encoding: str = "utf-8",
    normalize_headers: bool = True
) -> pd.DataFrame:
    """
    Lee un archivo CSV y retorna un DataFrame.
    """
    df = pd.read_csv(file_path, sep=sep, encoding=encoding)

    if normalize_headers:
        df = dataframe_normalize_headers(df)

    return df


def preview_csv(
    file_path: str,
    sep: str = ",",
    encoding: str = "utf-8",
    rows: int = 10
) -> pd.DataFrame:
    """
    Lee y retorna una vista previa del CSV.
    """
    df = read_csv_file(file_path, sep=sep, encoding=encoding)
    return df.head(rows)


def file_exists(file_path: str) -> bool:
    return Path(file_path).exists()