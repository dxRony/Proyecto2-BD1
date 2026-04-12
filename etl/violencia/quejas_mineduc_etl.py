import pandas as pd
from pathlib import Path

from repositories.firebird_repository import FirebirdRepository
from utils.normalizers import safe_int


FILE_PATH = "Violencia/Violencia contra la ninez/Quejas Mineduc/20240719123138C8M6SpIQkU1dO569us4WzmhiEojxPhwf.xlsx"


def get_or_create_fuente_dato(repo: FirebirdRepository) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("MINEDUC", "Quejas Mineduc", "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("MINEDUC", "Quejas Mineduc", "Excel"))

    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id


def get_departamento_id(repo: FirebirdRepository, nombre: str):
    repo.execute("""
        SELECT id
        FROM departamento
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    return row[0] if row else None


def get_or_create_tipo_agresion(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM tipo_agresion
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        INSERT INTO tipo_agresion (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id


def get_or_create_fecha(repo: FirebirdRepository, anio: int) -> int:
    repo.execute("""
        SELECT id
        FROM fecha
        WHERE anio = ? AND mes = 1 AND dia = 1
    """, (anio,))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fecha (fecha, anio, mes, nombre_mes, dia, dia_semana)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (f"{anio}-01-01", anio, 1, "Enero", 1, "Lunes"))

    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id


def build_dataframe_from_excel(file_path: str) -> pd.DataFrame:
    """
    Construye el DataFrame útil a partir de la hoja C1,
    ignorando filas decorativas y columnas basura.
    """
    raw = pd.read_excel(file_path, sheet_name="C1", header=None)

    raw = raw.iloc[:, :8].copy()

    raw.columns = [
        "departamento",
        "total",
        "embarazo_menor_14",
        "violencia_fisica_psicologica",
        "acoso_escolar_bullying",
        "acoso_y_hostigamiento_sexual",
        "racismo_y_discriminacion",
        "violencia_sexual",
    ]

    df = raw.iloc[4:].copy().reset_index(drop=True)

    df = df[df["departamento"].notna()]

    df = df[df["departamento"].astype(str).str.strip().str.lower() != "total"]

    return df


def melt_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte el formato ancho a largo.
    """
    df_melt = df.melt(
        id_vars=["departamento"],
        value_vars=[
            "embarazo_menor_14",
            "violencia_fisica_psicologica",
            "acoso_escolar_bullying",
            "acoso_y_hostigamiento_sexual",
            "racismo_y_discriminacion",
            "violencia_sexual",
        ],
        var_name="tipo_agresion",
        value_name="cantidad"
    )

    df_melt["cantidad"] = df_melt["cantidad"].fillna(0)
    df_melt["cantidad"] = df_melt["cantidad"].replace("-", 0)
    df_melt["cantidad"] = df_melt["cantidad"].apply(safe_int)
    df_melt = df_melt[df_melt["cantidad"].notna()]
    df_melt = df_melt[df_melt["cantidad"] > 0]

    return df_melt

def get_or_create_departamento(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM departamento
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        INSERT INTO departamento (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (None, nombre))

    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

def insert_queja(repo: FirebirdRepository, dep_id: int, fecha_id: int, tipo_id: int, cantidad: int, fuente_id: int):
    repo.execute("""
        INSERT INTO queja_mineduc_estadistica (
            id_departamento,
            id_fecha,
            id_tipo_agresion,
            cantidad,
            id_fuente_dato
        )
        VALUES (?, ?, ?, ?, ?)
    """, (dep_id, fecha_id, tipo_id, cantidad, fuente_id))


def run_quejas_mineduc_etl(repo: FirebirdRepository):
    if not Path(FILE_PATH).exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_PATH}")

    print(f"Procesando archivo: {FILE_PATH}")

    df = build_dataframe_from_excel(FILE_PATH)

    print("Vista previa del DataFrame base:")
    print(df.head())

    df_melt = melt_dataframe(df)

    print("Vista previa del DataFrame transformado:")
    print(df_melt.head(20))

    fuente_id = get_or_create_fuente_dato(repo)
    fecha_id = get_or_create_fecha(repo, 2023)

    inserted = 0
    skipped = 0

    for _, row in df_melt.iterrows():
        departamento = str(row["departamento"]).strip()
        tipo_agresion = str(row["tipo_agresion"]).strip()
        cantidad = int(row["cantidad"])

        dep_id = get_or_create_departamento(repo, departamento)

        tipo_id = get_or_create_tipo_agresion(repo, tipo_agresion)

        insert_queja(
            repo=repo,
            dep_id=dep_id,
            fecha_id=fecha_id,
            tipo_id=tipo_id,
            cantidad=cantidad,
            fuente_id=fuente_id
        )
        inserted += 1

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos: {skipped}")