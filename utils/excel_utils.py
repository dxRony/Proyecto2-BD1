import pandas as pd
from pathlib import Path
from utils.normalizers import dataframe_normalize_headers


def list_excel_sheets(file_path: str) -> list[str]:
    """
    Retorna la lista de hojas disponibles en un archivo Excel.
    """
    excel_file = pd.ExcelFile(file_path)
    return excel_file.sheet_names


def read_excel_file(
    file_path: str,
    sheet_name=0,
    header=0,
    normalize_headers: bool = True
) -> pd.DataFrame:
    """
    Lee un archivo Excel y retorna un DataFrame.
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header)

    if normalize_headers:
        df = dataframe_normalize_headers(df)

    return df


def preview_excel(
    file_path: str,
    sheet_name=0,
    header=0,
    rows: int = 10
) -> pd.DataFrame:
    """
    Lee y retorna una vista previa del Excel.
    """
    df = read_excel_file(file_path, sheet_name=sheet_name, header=header)
    return df.head(rows)


def file_exists(file_path: str) -> bool:
    return Path(file_path).exists()