from datetime import date
from pathlib import Path
import hashlib
import unicodedata
import re

import pandas as pd

from repositories.firebird_repository import FirebirdRepository


NOMBRES_MESES_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}

DIAS_ES = {
    0: "Lunes",
    1: "Martes",
    2: "Miércoles",
    3: "Jueves",
    4: "Viernes",
    5: "Sábado",
    6: "Domingo",
}


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
        text = str(value).strip()
        if normalize_name(text) in {"ignorada", "ignorado", "nan", "", "sd", "s/d"}:
            return None
        return int(float(text))
    except Exception:
        return None


def build_unique_code(text: str, prefix: str = "", max_len: int = 10) -> str:
    base = normalize_name(text).replace(" ", "_").upper()
    digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:3].upper()

    prefix = prefix.upper() if prefix else "X"

    reserve = len(prefix) + 1 + len(digest)
    cut_len = max_len - reserve
    if cut_len < 1:
        cut_len = 1

    trimmed = base[:cut_len]
    return f"{prefix}{trimmed}_{digest}"


def canonicalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "fecha",
        "dia",
        "mes",
        "anio",
        "sede",
        "municipio",
        "departamento",
        "edad",
        "rango_edad",
        "tipo_evaluacion",
        "orientacion_sexual",
        "valor",
    ]

    if len(df.columns) < len(expected_columns):
        raise ValueError(
            f"Se esperaban al menos {len(expected_columns)} columnas, pero llegaron {len(df.columns)}"
        )

    rename_map = {}
    for idx, new_name in enumerate(expected_columns):
        rename_map[df.columns[idx]] = new_name

    return df.rename(columns=rename_map)


def clean_catalog_value(value: str, default: str = "Ignorado") -> str:
    text = normalize_text(value)
    norm = normalize_name(text)

    if norm in {
        "",
        "sd",
        "s/d",
        "sin seleccion",
        "sin selección",
        "no registrado",
        "no registrada",
        "no registrados",
        "no registradas",
        "nan",
        "n/a",
        "na",
    }:
        return default

    return text


def parse_edad_years(value):
    text = normalize_text(value)
    if not text:
        return None

    norm = normalize_name(text)
    if norm in {"sd", "s/d", "ignorado", "ignorada", ""}:
        return None

    match = re.search(r"(\d+)", text)
    if not match:
        return None

    edad = safe_int(match.group(1))
    if edad is None:
        return None

    if edad < 0 or edad > 120:
        return None

    return edad


def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("INACIF", dataset_name, "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("INACIF", dataset_name, "Excel"))
    return repo.fetch_one()[0]


def get_or_create_fecha(repo: FirebirdRepository, anio: int, mes: int, dia: int):
    fecha_str = f"{anio:04d}-{mes:02d}-{dia:02d}"

    repo.execute("""
        SELECT id
        FROM fecha
        WHERE fecha = ?
    """, (fecha_str,))
    row = repo.fetch_one()
    if row:
        return row[0]

    fecha_obj = date(anio, mes, dia)

    repo.execute("""
        INSERT INTO fecha (fecha, anio, mes, nombre_mes, dia, dia_semana)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (
        fecha_str,
        anio,
        mes,
        NOMBRES_MESES_ES[mes],
        dia,
        DIAS_ES[fecha_obj.weekday()]
    ))
    return repo.fetch_one()[0]


def get_or_create_sexo(repo: FirebirdRepository, nombre: str) -> int:
    nombre_norm = normalize_name(nombre)

    if nombre_norm in {"hombre", "hombres", "masculino"}:
        codigo = "H"
        nombre_final = "Hombre"
    elif nombre_norm in {"mujer", "mujeres", "femenino"}:
        codigo = "M"
        nombre_final = "Mujer"
    else:
        codigo = "9"
        nombre_final = "Ignorado"

    repo.execute("""
        SELECT id
        FROM sexo
        WHERE codigo = ?
    """, (codigo,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO sexo (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre_final))
    return repo.fetch_one()[0]


def get_or_create_departamento_ignorado(repo: FirebirdRepository) -> int:
    repo.execute("""
        SELECT id
        FROM departamento
        WHERE codigo = ?
    """, ("9999",))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO departamento (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, ("9999", "Ignorado"))
    return repo.fetch_one()[0]


def get_or_create_municipio_ignorado(repo: FirebirdRepository) -> int:
    repo.execute("""
        SELECT id
        FROM municipio
        WHERE codigo = ?
    """, ("M99999",))
    row = repo.fetch_one()
    if row:
        return row[0]

    id_departamento = get_or_create_departamento_ignorado(repo)

    repo.execute("""
        INSERT INTO municipio (codigo, nombre, id_departamento)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("M99999", "Ignorado", id_departamento))
    return repo.fetch_one()[0]


def build_departamento_name_map(repo: FirebirdRepository) -> dict:
    get_or_create_departamento_ignorado(repo)

    repo.execute("""
        SELECT id, nombre
        FROM departamento
    """)
    rows = repo.fetch_all()

    result = {}
    for departamento_id, nombre in rows:
        result[normalize_name(nombre)] = departamento_id
    return result


def build_municipio_name_map(repo: FirebirdRepository) -> dict:
    get_or_create_municipio_ignorado(repo)

    repo.execute("""
        SELECT id, nombre, id_departamento
        FROM municipio
    """)
    rows = repo.fetch_all()

    result = {}
    for municipio_id, nombre, id_departamento in rows:
        key_full = f"{normalize_name(nombre)}|{id_departamento}"
        result[key_full] = municipio_id

        key_simple = normalize_name(nombre)
        if key_simple not in result:
            result[key_simple] = municipio_id

    return result


def get_or_create_grupo_etario(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM grupo_etario
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="G", max_len=10)

    repo.execute("""
        INSERT INTO grupo_etario (codigo, nombre, edad_min, edad_max, tipo_grupo)
        VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, None, None, "Evaluaciones INACIF Mujer"))
    return repo.fetch_one()[0]


def get_or_create_orientacion(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM orientacion
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO orientacion (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]


def get_or_create_clasificacion_evaluacion(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM clasificacion_evaluacion
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="CE", max_len=10)

    repo.execute("""
        INSERT INTO clasificacion_evaluacion (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None = None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]


def insert_evaluacion_medica(
    repo: FirebirdRepository,
    id_persona: int,
    id_fecha: int,
    id_municipio: int,
    id_clasificacion_evaluacion: int,
    id_orientacion: int | None,
    id_grupo_etario: int | None,
    id_fuente_dato: int,
):
    repo.execute("""
        INSERT INTO evaluacion_medica (
            id_persona,
            id_fecha,
            id_municipio,
            id_clasificacion_evaluacion,
            id_orientacion,
            id_condicion_edad,
            id_grupo_etario,
            id_fuente_dato,
            id_hecho_delictivo
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        id_persona,
        id_fecha,
        id_municipio,
        id_clasificacion_evaluacion,
        id_orientacion,
        None,
        id_grupo_etario,
        id_fuente_dato,
        None
    ))


def build_clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["departamento"] = df["departamento"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["municipio"] = df["municipio"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["sede"] = df["sede"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["edad_num"] = df["edad"].apply(parse_edad_years)
    df["rango_edad"] = df["rango_edad"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["tipo_evaluacion"] = df["tipo_evaluacion"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["orientacion_sexual"] = df["orientacion_sexual"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["valor"] = df["valor"].apply(lambda x: safe_int(x) or 0)

    df = df[df["fecha"].notna()].copy()
    df = df[df["valor"] > 0].copy()

    return df


def run_evaluaciones_inacif_mujer_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Evaluaciones realizadas por el INACIF 2008 al 2024"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")

    df = pd.read_excel(file_path, sheet_name="Evaluciones 2008-2024", header=0)
    df = canonicalize_dataframe_columns(df)
    df = build_clean_dataframe(df)

    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    departamento_name_map = build_departamento_name_map(repo)
    municipio_name_map = build_municipio_name_map(repo)
    sexo_id = get_or_create_sexo(repo, "Mujer")

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_departamento = 0
    skipped_missing_municipio = 0
    skipped_missing_clasificacion = 0

    for _, row in df.iterrows():
        fecha = row.get("fecha")
        if pd.isna(fecha):
            skipped_missing_fecha += 1
            continue

        anio = int(fecha.year)
        mes = int(fecha.month)
        dia = int(fecha.day)

        try:
            fecha_id = get_or_create_fecha(repo, anio, mes, dia)
        except Exception:
            skipped_missing_fecha += 1
            continue

        departamento_nombre = clean_catalog_value(row.get("departamento"), "Ignorado")
        departamento_id = departamento_name_map.get(normalize_name(departamento_nombre))

        if not departamento_id:
            skipped_missing_departamento += 1
            departamento_id = departamento_name_map.get("ignorado")

        if not departamento_id:
            departamento_id = get_or_create_departamento_ignorado(repo)

        municipio_nombre = clean_catalog_value(row.get("municipio"), "Ignorado")
        municipio_key_full = f"{normalize_name(municipio_nombre)}|{departamento_id}"
        municipio_id = municipio_name_map.get(municipio_key_full)

        if not municipio_id:
            municipio_id = municipio_name_map.get(normalize_name(municipio_nombre))

        if not municipio_id:
            skipped_missing_municipio += 1
            municipio_id = municipio_name_map.get("ignorado")

        if not municipio_id:
            municipio_id = get_or_create_municipio_ignorado(repo)

        edad = row.get("edad_num")
        if pd.isna(edad):
            edad = None
        else:
            edad = safe_int(edad)

        grupo_etario_nombre = clean_catalog_value(row.get("rango_edad"), "Ignorado")
        tipo_evaluacion_nombre = clean_catalog_value(row.get("tipo_evaluacion"), "Ignorado")
        orientacion_nombre = clean_catalog_value(row.get("orientacion_sexual"), "Ignorado")
        cantidad = safe_int(row.get("valor")) or 0

        try:
            grupo_etario_id = get_or_create_grupo_etario(repo, grupo_etario_nombre)
        except Exception:
            grupo_etario_id = None

        try:
            orientacion_id = get_or_create_orientacion(repo, orientacion_nombre)
        except Exception:
            orientacion_id = None

        try:
            clasificacion_id = get_or_create_clasificacion_evaluacion(repo, tipo_evaluacion_nombre)
        except Exception:
            skipped_missing_clasificacion += 1
            continue

        if cantidad <= 0:
            continue

        for _ in range(cantidad):
            persona_id = create_persona(repo, sexo_id, edad)

            insert_evaluacion_medica(
                repo=repo,
                id_persona=persona_id,
                id_fecha=fecha_id,
                id_municipio=municipio_id,
                id_clasificacion_evaluacion=clasificacion_id,
                id_orientacion=orientacion_id,
                id_grupo_etario=grupo_etario_id,
                id_fuente_dato=fuente_id,
            )

            inserted += 1

            if inserted % 1000 == 0:
                print(f"Procesados correctamente: {inserted}")

    repo.commit()

    print(f"Fuente de dato usada: {fuente_id}")
    print(f"Insertados: {inserted}")
    print(f"Omitidos por fecha inválida: {skipped_missing_fecha}")
    print(f"Omitidos por departamento no encontrado: {skipped_missing_departamento}")
    print(f"Omitidos por municipio no encontrado: {skipped_missing_municipio}")
    print(f"Omitidos por clasificación inválida: {skipped_missing_clasificacion}")