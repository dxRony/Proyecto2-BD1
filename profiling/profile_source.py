from pathlib import Path
import argparse
import pandas as pd

from utils.normalizers import normalize_text


CANDIDATE_KEYWORDS = [
    "departamento",
    "depto",
    "municipio",
    "codigo",
    "cod",
    "fecha",
    "anio",
    "año",
    "mes",
    "sexo",
    "delito",
    "agresion",
    "cantidad",
    "casos",
    "grupo",
    "edad",
    "tipo",
    "estado",
    "fuente"
]


def print_separator(title: str = "", width: int = 90):
    line = "=" * width
    if title:
        print(f"\n{line}\n{title}\n{line}")
    else:
        print(f"\n{line}")


def detect_candidate_columns(columns: list[str]) -> list[str]:
    matches = []

    for col in columns:
        normalized = normalize_text(col) or ""
        for keyword in CANDIDATE_KEYWORDS:
            if keyword in normalized:
                matches.append(col)
                break

    return matches


def summarize_dataframe(df: pd.DataFrame, sheet_name: str | None = None, preview_rows: int = 10):
    title = f"Resumen de hoja: {sheet_name}" if sheet_name is not None else "Resumen de archivo"
    print_separator(title)

    print(f"Shape: {df.shape}")
    print("\nPreview:")
    print(df.head(preview_rows))

    print_separator("Columnas")
    for idx, col in enumerate(df.columns):
        print(f"{idx}: {col}")

    print_separator("Tipos de datos")
    print(df.dtypes)

    print_separator("Nulos por columna")
    print(df.isna().sum())

    candidate_columns = detect_candidate_columns([str(c) for c in df.columns])

    print_separator("Columnas candidatas para ETL")
    if candidate_columns:
        for col in candidate_columns:
            print(f"- {col}")
    else:
        print("No se detectaron columnas candidatas por nombre.")

    if candidate_columns:
        print_separator("Valores únicos (muestra) de columnas candidatas")
        for col in candidate_columns:
            uniques = df[col].dropna().astype(str).str.strip().unique().tolist()
            print(f"\n{col}:")
            for value in uniques[:20]:
                print(f"  - {value}")
            if len(uniques) > 20:
                print(f"  ... y {len(uniques) - 20} más")


def profile_excel(file_path: str, preview_rows: int = 10, max_sheets: int | None = None):
    print_separator("Perfilando Excel")
    print(f"Archivo: {file_path}")

    excel_file = pd.ExcelFile(file_path)
    sheets = excel_file.sheet_names

    print("\nHojas encontradas:")
    for sheet in sheets:
        print(f"- {sheet}")

    if max_sheets is not None:
        sheets = sheets[:max_sheets]

    for sheet in sheets:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet, header=None)
            print_separator(f"Hoja cruda: {sheet}")
            print(f"Shape crudo: {df.shape}")
            print(df.head(preview_rows))

            if df.shape[0] > 3:
                try:
                    df_header3 = pd.read_excel(file_path, sheet_name=sheet, header=3)
                    summarize_dataframe(df_header3, sheet_name=f"{sheet} (header=3)", preview_rows=preview_rows)
                except Exception as e:
                    print(f"\nNo se pudo leer {sheet} con header=3: {e}")

        except Exception as e:
            print(f"Error leyendo hoja {sheet}: {e}")


def profile_csv(file_path: str, preview_rows: int = 10, sep: str = ",", encoding: str = "utf-8"):
    print_separator("Perfilando CSV")
    print(f"Archivo: {file_path}")

    try:
        df = pd.read_csv(file_path, sep=sep, encoding=encoding)
        summarize_dataframe(df, sheet_name=None, preview_rows=preview_rows)
    except UnicodeDecodeError:
        print("Falló UTF-8. Probando latin-1...")
        df = pd.read_csv(file_path, sep=sep, encoding="latin-1")
        summarize_dataframe(df, sheet_name=None, preview_rows=preview_rows)


def main():
    parser = argparse.ArgumentParser(description="Perfilador de fuentes para ETL")
    parser.add_argument("--file", required=True, help="Ruta del archivo a perfilar")
    parser.add_argument("--rows", type=int, default=10, help="Cantidad de filas preview")
    parser.add_argument("--sep", default=",", help="Separador para CSV")
    parser.add_argument("--encoding", default="utf-8", help="Encoding para CSV")
    parser.add_argument("--max-sheets", type=int, default=None, help="Máximo de hojas a inspeccionar en Excel")

    args = parser.parse_args()

    file_path = args.file

    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    extension = Path(file_path).suffix.lower()

    if extension in [".xlsx", ".xls"]:
        profile_excel(file_path, preview_rows=args.rows, max_sheets=args.max_sheets)
    elif extension == ".csv":
        profile_csv(file_path, preview_rows=args.rows, sep=args.sep, encoding=args.encoding)
    else:
        raise ValueError(f"Formato no soportado: {extension}")


if __name__ == "__main__":
    main()