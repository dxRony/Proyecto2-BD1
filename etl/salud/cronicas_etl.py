from pathlib import Path
import pandas as pd

from repositories.firebird_repository import FirebirdRepository
from utils.csv_utils import read_csv_file
from utils.normalizers import normalize_text, safe_int

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_cols = []

    for col in df.columns:
        col_norm = normalize_text(col)  # quita tildes y pasa a minúsculas
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

def normalize_sexo(value: str) -> tuple[str, str]:
    val = normalize_text(value)

    if val in ["m", "masculino"]:
        return "M", "Masculino"
    if val in ["f", "femenino"]:
        return "F", "Femenino"

    return "ND", "No definido"

def build_dataframe(file_path: str) -> pd.DataFrame:
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
        "sexo",
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
    df["sexo"] = df["sexo"].astype(str).str.strip()

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

def run_cronicas_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Enfermedades crónicas"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(file_path)

    print(f"Procesando: {file_path}")

    df = build_dataframe(file_path)

    print("Vista previa del DataFrame limpio:")
    print(df.head(10))
    print(f"Total filas: {len(df)}")

    fuente_id = repo.get_or_create_fuente_dato("MSPAS", dataset_name, "CSV")
    tipo_indicador_id = repo.get_or_create_tipo_indicador_salud("Enfermedades crónicas")

    inserted = 0
    skipped = 0

    id_enfermedad_fija = repo.get_or_create_enfermedad(dataset_name, "Salud")
    
    for _, row in df.iterrows():
        try:
            sexo_codigo, sexo_nombre = normalize_sexo(row["sexo"])

            id_departamento = repo.get_or_create_departamento(row["departamento"])
            id_municipio = repo.get_or_create_municipio(row["municipio"], id_departamento)
            id_grupo_etario = repo.get_or_create_grupo_etario(row["grupo_etario"])
            id_sexo = repo.get_or_create_sexo(sexo_codigo, sexo_nombre)
            id_fecha = repo.get_or_create_fecha(int(row["anio"]))
            id_diagnostico = get_or_create_diagnostico(repo, row["cie10"], row["diagnostico"])
            id_enfermedad=id_enfermedad_fija

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
            print(f"Fila omitida por error: {row.to_dict()} -> {e}")

    repo.commit()
    print(f"FINAL -> Insertados: {inserted}")
    print(f"FINAL -> Omitidos: {skipped}")