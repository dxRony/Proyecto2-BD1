from pathlib import Path
import pandas as pd

from repositories.firebird_repository import FirebirdRepository
from utils.csv_utils import read_csv_file
from utils.normalizers import normalize_text, safe_int

# normalizers

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("ñ", "n")
    )

    rename_map = {
        "casos": "cantidad",
        "grupo_etario": "grupo_etario",
        "grupo_etario_": "grupo_etario",
        "grupo": "grupo_etario"
    }

    df = df.rename(columns=rename_map)

    return df


def normalize_sexo(value: str) -> tuple[str, str]:
    val = normalize_text(value)

    if val in ["m", "masculino"]:
        return "M", "Masculino"
    if val in ["f", "femenino"]:
        return "F", "Femenino"

    return "ND", "No definido"


def normalize_grupo_etario(value: str) -> str:
    if not value:
        return None

    val = normalize_text(value)
    val = val.replace("  ", " ")
    val = val.replace("a  a", "a a")

    return val.strip()


#procesador etl
def build_dataframe(file_path: str) -> pd.DataFrame:
    df = read_csv_file(
        file_path=file_path,
        sep=";",
        encoding="utf-8-sig",
        normalize_headers=False
    )

    df = normalize_columns(df)

    required = ["anio", "departamento", "municipio", "grupo_etario", "sexo", "cantidad"]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas: {missing} -> {list(df.columns)}")

    df = df[required].copy()

    df["anio"] = df["anio"].apply(safe_int)
    df["cantidad"] = df["cantidad"].apply(safe_int)

    df["departamento"] = df["departamento"].astype(str).str.strip().str.title()
    df["municipio"] = df["municipio"].astype(str).str.strip().str.title()
    df["grupo_etario"] = df["grupo_etario"].apply(normalize_grupo_etario)
    df["sexo"] = df["sexo"].astype(str).str.strip()

    df = df.dropna(subset=["anio", "cantidad"])
    df = df[df["cantidad"] > 0]

    return df



# ejecutor elt

def run_salud_etl(
    repo: FirebirdRepository,
    file_path: str,
    enfermedad_nombre: str,
    tipo_indicador_nombre: str
):
    if not Path(file_path).exists():
        raise FileNotFoundError(file_path)

    print(f"Procesando: {file_path}")

    df = build_dataframe(file_path)

    print(df.head())
    print(f"Total filas: {len(df)}")

    fuente_id = repo.get_or_create_fuente_dato("MSPAS", enfermedad_nombre, "CSV")
    tipo_indicador_id = repo.get_or_create_tipo_indicador_salud(tipo_indicador_nombre)
    enfermedad_id = repo.get_or_create_enfermedad(enfermedad_nombre, "Vectores")

    inserted = 0

    for _, row in df.iterrows():
        try:
            sexo_codigo, sexo_nombre = normalize_sexo(row["sexo"])

            id_departamento = repo.get_or_create_departamento(row["departamento"])
            id_municipio = repo.get_or_create_municipio(row["municipio"], id_departamento)
            id_grupo_etario = repo.get_or_create_grupo_etario(row["grupo_etario"])
            id_sexo = repo.get_or_create_sexo(sexo_codigo, sexo_nombre)
            id_fecha = repo.get_or_create_fecha(row["anio"])

            repo.insert_registro_salud(
                tipo_indicador_id,
                enfermedad_id,
                None,
                id_municipio,
                id_fecha,
                id_grupo_etario,
                id_sexo,
                int(row["cantidad"]),
                fuente_id
            )

            inserted += 1

            if inserted % 1000 == 0:
                repo.commit()
                print(f"Insertados: {inserted}")

        except Exception as e:
            print("Error:", e)

    repo.commit()

    print(f"FINAL → Insertados: {inserted}")