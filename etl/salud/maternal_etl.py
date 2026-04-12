from pathlib import Path
import pandas as pd

from repositories.firebird_repository import FirebirdRepository
from utils.csv_utils import read_csv_file
from utils.normalizers import normalize_text, safe_int


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_cols = []

    for col in df.columns:
        col_norm = normalize_text(col)
        col_norm = col_norm.replace(" ", "_").replace("-", "_")
        normalized_cols.append(col_norm)

    df.columns = normalized_cols

    rename_map = {
        "grupoetario": "grupo_etario",
        "grupo_etario": "grupo_etario",
        "cie_10": "cie10",
        "casos": "cantidad",
    }

    return df.rename(columns=rename_map)


def normalize_grupo_etario(value: str) -> str:
    if not value:
        return None
    val = normalize_text(value)
    val = val.replace("  ", " ").strip()
    return val


def build_dataframe(file_path: str) -> pd.DataFrame:
    if not Path(file_path).exists():
        raise FileNotFoundError(file_path)

    df = read_csv_file(
        file_path=file_path,
        sep=";",
        encoding="utf-8-sig",
        normalize_headers=False
    )

    df = normalize_columns(df)

    required = [
        "anio",
        "departamento",
        "municipio",
        "cie10",
        "diagnostico",
        "grupo_etario",
        "cantidad",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}. Columnas actuales: {list(df.columns)}")

    df = df[required].copy()

    df["anio"] = df["anio"].apply(safe_int)
    df["cantidad"] = df["cantidad"].apply(safe_int)

    df["departamento"] = df["departamento"].astype(str).str.strip().str.title()
    df["municipio"] = df["municipio"].astype(str).str.strip().str.title()
    df["cie10"] = df["cie10"].astype(str).str.strip()
    df["diagnostico"] = df["diagnostico"].astype(str).str.strip()
    df["grupo_etario"] = df["grupo_etario"].apply(normalize_grupo_etario)

    df = df.dropna(subset=["anio", "cantidad"])
    df = df[df["cantidad"] > 0]

    return df

def get_or_create_diagnostico(repo: FirebirdRepository, codigo: str, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM diagnostico_cie10
        WHERE LOWER(codigo) = LOWER(?)
           OR LOWER(nombre) = LOWER(?)
    """, (codigo, nombre))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        INSERT INTO diagnostico_cie10 (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

def run_maternal_etl(repo: FirebirdRepository, file_path: str):
    df = build_dataframe(file_path)

    print(df.head())
    print(f"Total filas: {len(df)}")

    fuente_id = repo.get_or_create_fuente_dato("MSPAS", "Morbilidad materna", "CSV")
    tipo_indicador_id = repo.get_or_create_tipo_indicador_salud("Morbilidad materna")

    inserted = 0
    skipped = 0

    id_enfermedad = repo.get_or_create_enfermedad("Morbilidad materna", "Materna")
    
    for _, row in df.iterrows():
        try:
            id_departamento = repo.get_or_create_departamento(row["departamento"])
            id_municipio = repo.get_or_create_municipio(row["municipio"], id_departamento)
            id_grupo_etario = repo.get_or_create_grupo_etario(row["grupo_etario"])
            id_fecha = repo.get_or_create_fecha(int(row["anio"]))

            id_sexo = repo.get_or_create_sexo("ND", "No definido")

            id_diagnostico = get_or_create_diagnostico(repo, row["cie10"], row["diagnostico"])

            repo.insert_registro_salud(
                id_tipo_indicador_salud=tipo_indicador_id,
                id_enfermedad=id_enfermedad,
                id_diagnostico=id_diagnostico,
                id_municipio=id_municipio,
                id_fecha=id_fecha,
                id_grupo_etario=id_grupo_etario,
                id_sexo=id_sexo,
                cantidad=int(row["cantidad"]),
                id_fuente_dato=fuente_id
            )

            inserted += 1

            if inserted % 1000 == 0:
                repo.commit()
                print(f"Insertados: {inserted}")

        except Exception as e:
            skipped += 1
            print(f"Error: {e}")

    repo.commit()
    print(f"FINAL -> Insertados: {inserted}")
    print(f"FINAL -> Omitidos: {skipped}")