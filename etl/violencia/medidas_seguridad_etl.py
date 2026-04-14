from datetime import date
from pathlib import Path
import pandas as pd
import unicodedata
import hashlib

from repositories.firebird_repository import FirebirdRepository


# =========================
# Normalización
# =========================

def normalize_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalize_name(text) -> str:
    if text is None or pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = " ".join(text.split())
    return text


def safe_int(value):
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def clean_catalog_value(value: str, default: str = "Ignorado") -> str:
    text = normalize_text(value)
    norm = normalize_name(text)

    if norm in {"", "sd", "s/d", "nan"}:
        return default

    return text


# =========================
# Fecha
# =========================

NOMBRES_MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

DIAS_ES = {
    0: "Lunes", 1: "Martes", 2: "Miércoles",
    3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo",
}


def get_or_create_fecha(repo: FirebirdRepository, anio: int):
    fecha_str = f"{anio:04d}-01-01"

    repo.execute("SELECT id FROM fecha WHERE fecha = ?", (fecha_str,))
    row = repo.fetch_one()
    if row:
        return row[0]

    fecha_obj = date(anio, 1, 1)

    repo.execute("""
        INSERT INTO fecha (fecha, anio, mes, nombre_mes, dia, dia_semana)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (
        fecha_str,
        anio,
        1,
        NOMBRES_MESES_ES[1],
        1,
        DIAS_ES[fecha_obj.weekday()]
    ))

    return repo.fetch_one()[0]


# =========================
# Departamento / Municipio
# =========================

def get_or_create_departamento_ignorado(repo: FirebirdRepository):
    repo.execute("SELECT id FROM departamento WHERE codigo = ?", ("9999",))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO departamento (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, ("9999", "Ignorado"))
    return repo.fetch_one()[0]


def get_or_create_municipio_ignorado(repo: FirebirdRepository):
    repo.execute("SELECT id FROM municipio WHERE codigo = ?", ("M99999",))
    row = repo.fetch_one()
    if row:
        return row[0]

    id_dep = get_or_create_departamento_ignorado(repo)

    repo.execute("""
        INSERT INTO municipio (codigo, nombre, id_departamento)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("M99999", "Ignorado", id_dep))

    return repo.fetch_one()[0]


def build_departamento_name_map(repo: FirebirdRepository):
    get_or_create_departamento_ignorado(repo)

    repo.execute("SELECT id, nombre FROM departamento")
    rows = repo.fetch_all()

    result = {}
    for dep_id, nombre in rows:
        result[normalize_name(nombre)] = dep_id
    return result


def build_municipio_name_map(repo: FirebirdRepository):
    get_or_create_municipio_ignorado(repo)

    repo.execute("SELECT id, nombre, id_departamento FROM municipio")
    rows = repo.fetch_all()

    result = {}

    for mun_id, nombre, dep_id in rows:
        key_full = f"{normalize_name(nombre)}|{dep_id}"
        result[key_full] = mun_id

        key_simple = normalize_name(nombre)
        if key_simple not in result:
            result[key_simple] = mun_id

    return result


# =========================
# Despacho judicial
# =========================

def get_or_create_despacho(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")
    nombre = nombre[:150]

    repo.execute("""
        SELECT id
        FROM despacho_judicial
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO despacho_judicial (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))

    return repo.fetch_one()[0]


# =========================
# Insert
# =========================

def insert_medida(
    repo: FirebirdRepository,
    id_municipio: int,
    id_despacho: int,
    id_fecha: int,
    cantidad: int
):
    repo.execute("""
        INSERT INTO medidas_seguridad_estadistica (
            id_municipio,
            id_despacho,
            id_fecha,
            cantidad
        )
        VALUES (?, ?, ?, ?)
    """, (
        id_municipio,
        id_despacho,
        id_fecha,
        cantidad
    ))


# =========================
# Canonicalización
# =========================

def canonicalize_dataframe_columns(df: pd.DataFrame):
    expected = [
        "departamento",
        "municipio",
        "despacho",
        "anio",
        "valor"
    ]

    rename_map = {}
    for i, col in enumerate(df.columns[:5]):
        rename_map[col] = expected[i]

    return df.rename(columns=rename_map)


# =========================
# ETL principal
# =========================

def run_medidas_seguridad_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Medidas de Seguridad 2012-2024"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(file_path)

    print(f"Procesando: {file_path}")

    df = pd.read_excel(file_path, sheet_name="Medidas de seguridad", header=0)
    df = canonicalize_dataframe_columns(df)

    df["departamento"] = df["departamento"].apply(lambda x: clean_catalog_value(x))
    df["municipio"] = df["municipio"].apply(lambda x: clean_catalog_value(x))
    df["despacho"] = df["despacho"].apply(lambda x: clean_catalog_value(x))
    df["anio"] = df["anio"].apply(safe_int)
    df["valor"] = df["valor"].apply(lambda x: safe_int(x) or 0)

    df = df[df["anio"].notna()]
    df = df[df["valor"] > 0]

    df = df.groupby(
        ["departamento", "municipio", "despacho", "anio"],
        as_index=False
    )["valor"].sum().rename(columns={"valor": "cantidad"})

    departamento_map = build_departamento_name_map(repo)
    municipio_map = build_municipio_name_map(repo)

    inserted = 0
    skipped_municipio = 0

    for _, row in df.iterrows():
        anio = row["anio"]
        fecha_id = get_or_create_fecha(repo, anio)

        dep_name = normalize_name(row["departamento"])
        dep_id = departamento_map.get(dep_name)

        mun_name = normalize_name(row["municipio"])

        mun_key = f"{mun_name}|{dep_id}"
        municipio_id = municipio_map.get(mun_key)

        if not municipio_id:
            municipio_id = municipio_map.get(mun_name)

        if not municipio_id:
            skipped_municipio += 1
            municipio_id = get_or_create_municipio_ignorado(repo)

        despacho_id = get_or_create_despacho(repo, row["despacho"])

        cantidad = row["cantidad"]

        insert_medida(
            repo,
            municipio_id,
            despacho_id,
            fecha_id,
            cantidad
        )

        inserted += 1

        if inserted % 1000 == 0:
            print(f"Procesados: {inserted}")

    repo.commit()

    print("ETL finalizado")
    print(f"Insertados: {inserted}")
    print(f"Municipios no encontrados: {skipped_municipio}")