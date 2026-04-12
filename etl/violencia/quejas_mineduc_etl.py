import unicodedata
from pathlib import Path

import pandas as pd

from repositories.firebird_repository import FirebirdRepository
from utils.normalizers import safe_int


FILE_PATH = "Violencia/Violencia contra la ninez/Quejas Mineduc/20240719123138C8M6SpIQkU1dO569us4WzmhiEojxPhwf.xlsx"

TIPOS_AGRESION_MAP = {
    "embarazo_menor_14": "Embarazo en menor de 14 años",
    "violencia_fisica_psicologica": "Violencia física y psicológica",
    "acoso_escolar_bullying": "Acoso escolar (bullying)",
    "acoso_y_hostigamiento_sexual": "Acoso y hostigamiento sexual",
    "racismo_y_discriminacion": "Racismo y discriminación",
    "violencia_sexual": "Violencia sexual",
    "abuso_de_autoridad": "Abuso de autoridad",
}

DEPARTAMENTO_ALIASES = {
    "peten": "el peten",
}

def normalize_text(text: str) -> str:
    if text is None:
        return ""

    text = str(text).strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text


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

    return repo.fetch_one()[0]


def get_fecha_id(repo: FirebirdRepository, anio: int):
    repo.execute("""
        SELECT id
        FROM fecha
        WHERE fecha = ?
    """, (f"{anio}-01-01",))

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

    return repo.fetch_one()[0]


def build_departamento_map(repo: FirebirdRepository) -> dict:
    repo.execute("""
        SELECT id, nombre
        FROM departamento
    """)
    rows = repo.fetch_all()

    dep_map = {}
    for dep_id, nombre in rows:
        dep_map[normalize_text(nombre)] = dep_id

    return dep_map


def exists_queja(repo: FirebirdRepository, dep_id: int, fecha_id: int, tipo_id: int, fuente_id: int) -> bool:
    repo.execute("""
        SELECT 1
        FROM queja_mineduc_estadistica
        WHERE id_departamento = ?
          AND id_fecha = ?
          AND id_tipo_agresion = ?
          AND id_fuente_dato = ?
    """, (dep_id, fecha_id, tipo_id, fuente_id))

    return repo.fetch_one() is not None


def insert_queja(
    repo: FirebirdRepository,
    dep_id: int,
    fecha_id: int,
    tipo_id: int,
    cantidad: int,
    fuente_id: int
):
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


def build_dataframe_from_excel(file_path: str) -> pd.DataFrame:
    raw = pd.read_excel(file_path, sheet_name="C1", header=None)

    # columnas útiles reales según profiling
    raw = raw.iloc[:, :9].copy()

    raw.columns = [
        "departamento",
        "total",
        "embarazo_menor_14",
        "violencia_fisica_psicologica",
        "acoso_escolar_bullying",
        "acoso_y_hostigamiento_sexual",
        "racismo_y_discriminacion",
        "violencia_sexual",
        "abuso_de_autoridad",
    ]
    # quitar encabezados basura
    df = raw.iloc[4:].copy().reset_index(drop=True)
    # quitar filas empt
    df = df[df["departamento"].notna()]
    # quitar fila total
    df = df[df["departamento"].astype(str).str.strip().str.lower() != "total"]

    return df


def melt_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_melt = df.melt(
        id_vars=["departamento"],
        value_vars=[
            "embarazo_menor_14",
            "violencia_fisica_psicologica",
            "acoso_escolar_bullying",
            "acoso_y_hostigamiento_sexual",
            "racismo_y_discriminacion",
            "violencia_sexual",
            "abuso_de_autoridad",
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


def run_quejas_mineduc_etl(repo: FirebirdRepository):
    if not Path(FILE_PATH).exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_PATH}")

    print(f"Procesando archivo: {FILE_PATH}")

    df = build_dataframe_from_excel(FILE_PATH)
    df_melt = melt_dataframe(df)

    fuente_id = get_or_create_fuente_dato(repo)
    fecha_id = get_fecha_id(repo, 2023)

    if not fecha_id:
        raise ValueError("No existe la fecha 2023-01-01 en la tabla fecha")

    dep_map = build_departamento_map(repo)

    inserted = 0
    skipped = 0

    for _, row in df_melt.iterrows():
        departamento_original = str(row["departamento"]).strip()
        departamento_key = normalize_text(departamento_original)
        departamento_key = DEPARTAMENTO_ALIASES.get(departamento_key, departamento_key)

        dep_id = dep_map.get(departamento_key)
        if not dep_id:
            print(f"Departamento no encontrado: {departamento_original}")
            skipped += 1
            continue

        tipo_agresion_key = str(row["tipo_agresion"]).strip()
        tipo_agresion_nombre = TIPOS_AGRESION_MAP[tipo_agresion_key]
        tipo_id = get_or_create_tipo_agresion(repo, tipo_agresion_nombre)

        cantidad = int(row["cantidad"])

        if exists_queja(repo, dep_id, fecha_id, tipo_id, fuente_id):
            skipped += 1
            continue

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