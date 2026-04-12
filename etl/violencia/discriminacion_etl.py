import unicodedata
from pathlib import Path

import pandas as pd

from repositories.firebird_repository import FirebirdRepository


FILE_PATH = "Violencia/Violencia estructural/CASOS DISCRIMINACIÓN 2016-2023.xls"

SHEETS = ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"]

DEPARTAMENTO_ALIASES = {
    "peten": "el peten",
    "quiche": "quiche",
    "quetaltenango": "quetzaltenango",
    "quetaltenango.": "quetzaltenango",
    "sacatepequez": "sacatepequez",
    "totonicapan": "totonicapan",
    "solola": "solola",
    "huehuetenango": "huehuetenango",
    "alta verapaz": "alta verapaz",
    "san marcos": "san marcos",
    "guatemala": "guatemala",
    "chimaltenango": "chimaltenango",
    "chiquimula": "chiquimula",
}

TIPO_DISCRIMINACION_MAP = {
    "etnica": "Étnica",
    "racial": "Racial",
    "etnica/ genero": "Étnica y Género",
    "etnica/genero": "Étnica y Género",
    "racial y etnica": "Racial y Étnica",
    "discriminacion": "Discriminación",
    "discriminacion etnica": "Discriminación Étnica",
    "etnica laboral": "Étnica laboral",
    "etnica racial": "Étnica racial",
}

GRUPO_ETNICO_MAP = {
    "maya": "Maya",
    "garifuna": "Garífuna",
    "xinka": "Xinka",
    "xinca": "Xinka",
    "k'iche'": "K'iche'",
    "k´iche´": "K'iche'",
    "ki'che'": "K'iche'",
    "q'eqchi'": "Q'eqchi'",
    "q`eqchi`": "Q'eqchi'",
    "q'qechi'": "Q'eqchi'",
    "poqomch'": "Poqomchí",
    "poqomchi": "Poqomchí",
    "kaqchikel": "Kaqchikel",
    "mam": "Mam",
    "q'anjob'al": "Q'anjob'al",
    "q'anjobal": "Q'anjob'al",
    "jakalteka": "Jakalteko",
    "nahuatl": "Náhuatl",
    "chorti": "Chortí",
    "tz´utujil": "Tz'utujil",
    "tzutujil": "Tz'utujil",
    "no indica": "No indica",
}

COMUNIDAD_MAP = {
    "k'iche'": "K'iche'",
    "k´iche´": "K'iche'",
    "ki'che'": "K'iche'",
    "q'eqchi'": "Q'eqchi'",
    "q`eqchi`": "Q'eqchi'",
    "q'qechi'": "Q'eqchi'",
    "kaqchikel": "Kaqchikel",
    "mam": "Mam",
    "q'anjob'al": "Q'anjob'al",
    "q'anjobal": "Q'anjob'al",
    "jakalteka": "Jakalteko",
    "nahuatl": "Náhuatl",
    "poqomch'": "Poqomchí",
    "poqomchi": "Poqomchí",
    "chorti": "Chortí",
    "xinca": "Xinka",
    "xinka": "Xinka",
    "garifuna": "Garífuna",
    "idioma garifuna": "Idioma Garífuna",
    "no indica": "No indica",
}

def normalize_text(text) -> str:
    if text is None or pd.isna(text):
        return ""

    text = str(text).strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.replace("`", "'")
    text = " ".join(text.split())
    return text

def is_marked(value) -> bool:
    if value is None or pd.isna(value):
        return False

    val = normalize_text(value)
    return val in {"1", "x"}

def parse_edad(value):
    if value is None or pd.isna(value):
        return None

    val = normalize_text(value)

    if val in {"", "no indica", "nan"}:
        return None

    if "-" in val:
        return None

    try:
        return int(float(str(value).strip()))
    except Exception:
        return None

def parse_sexo_old(row) -> str | None:
    """
    Formato 2016-2019:
    col 1 = M/H
    col 2 = F/M
    """
    val1 = row.iloc[1]
    val2 = row.iloc[2]

    if is_marked(val1):
        return "Masculino"
    if is_marked(val2):
        return "Femenino"
    return None

def parse_sexo_new(row) -> str | None:
    """
    Formato 2020-2023:
    col 1 = M
    col 2 = H
    """
    val_m = row.iloc[1]
    val_h = row.iloc[2]

    if is_marked(val_m):
        return "Mujer"
    if is_marked(val_h):
        return "Hombre"
    return None

def normalize_departamento(value: str) -> str | None:
    val = normalize_text(value)

    if val in {"", "departamento", "no indica", "nan", "santiago atitlan"}:
        return None

    val = DEPARTAMENTO_ALIASES.get(val, val)
    return val

def normalize_tipo_discriminacion(value: str) -> str | None:
    val = normalize_text(value)

    if val in {"", "nan"}:
        return None

    val = val.replace("/", "/ ")
    val = " ".join(val.split())

    if val in TIPO_DISCRIMINACION_MAP:
        return TIPO_DISCRIMINACION_MAP[val]

    if "racial" in val and "etnica" in val:
        return "Racial y Étnica"
    if "etnica" in val and "genero" in val:
        return "Étnica y Género"
    if "discriminacion" in val and "etnica" in val:
        return "Discriminación Étnica"
    if "etnica" in val and "laboral" in val:
        return "Étnica laboral"
    if "etnica" in val and "racial" in val:
        return "Étnica racial"
    if "etnica" in val:
        return "Étnica"
    if "racial" in val:
        return "Racial"
    if "discriminacion" in val:
        return "Discriminación"

    return str(value).strip()

def normalize_grupo_etnico(value: str) -> str | None:
    val = normalize_text(value)

    if val in {"", "nan"}:
        return None

    return GRUPO_ETNICO_MAP.get(val, str(value).strip())


def normalize_comunidad(value: str) -> str | None:
    val = normalize_text(value)

    if val in {"", "nan"}:
        return None

    return COMUNIDAD_MAP.get(val, str(value).strip())

def extract_grupo_etnico_old(row) -> str | None:
    return normalize_grupo_etnico(row.iloc[6])

def extract_grupo_etnico_new(row) -> str | None:
    maya = row.iloc[4]
    garifuna = row.iloc[5]
    xinka = row.iloc[6]

    if is_marked(maya):
        return "Maya"
    if is_marked(garifuna):
        return "Garífuna"
    if is_marked(xinka):
        return "Xinka"
    return None

def get_or_create_fuente_dato(repo: FirebirdRepository) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("CODISRA", "Casos de discriminación 2016-2023", "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("CODISRA", "Casos de discriminación 2016-2023", "Excel"))

    return repo.fetch_one()[0]

def get_fecha_id(repo: FirebirdRepository, anio: int):
    repo.execute("""
        SELECT id
        FROM fecha
        WHERE fecha = ?
    """, (f"{anio}-01-01",))

    row = repo.fetch_one()
    return row[0] if row else None

def build_departamento_map(repo: FirebirdRepository) -> dict:
    repo.execute("""
        SELECT id, nombre
        FROM departamento
    """)
    rows = repo.fetch_all()

    result = {}
    for dep_id, nombre in rows:
        result[normalize_text(nombre)] = dep_id

    return result

def get_or_create_sexo(repo: FirebirdRepository, nombre: str) -> int:
    nombre_norm = normalize_text(nombre)

    if nombre_norm in {"hombre", "masculino"}:
        codigo = "H"
        nombre_canonico = "Hombre"
    elif nombre_norm in {"mujer", "femenino"}:
        codigo = "M"
        nombre_canonico = "Mujer"
    else:
        raise ValueError(f"Sexo no reconocido: {nombre}")

    # primero busca codigo
    repo.execute("""
        SELECT id
        FROM sexo
        WHERE codigo = ?
    """, (codigo,))
    row = repo.fetch_one()
    if row:
        return row[0]

    # si no existe crea
    repo.execute("""
        INSERT INTO sexo (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre_canonico))
    return repo.fetch_one()[0]

def get_or_create_tipo_discriminacion(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM tipo_discriminacion
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO tipo_discriminacion (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]


def get_or_create_grupo_etnico(repo: FirebirdRepository, nombre: str | None):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM grupo_etnico
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO grupo_etnico (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]


def get_or_create_comunidad_linguistica(repo: FirebirdRepository, nombre: str | None):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM comunidad_linguistica
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO comunidad_linguistica (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]


def create_persona(repo: FirebirdRepository, id_sexo: int | None, edad: int | None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]


def exists_caso_discriminacion(
    repo: FirebirdRepository,
    fecha_id: int,
    persona_id: int,
    departamento_id: int | None,
    tipo_discriminacion_id: int | None,
    grupo_etnico_id: int | None,
    comunidad_linguistica_id: int | None,
    fuente_id: int
) -> bool:
    repo.execute("""
        SELECT 1
        FROM caso_discriminacion
        WHERE id_fecha_aproximada = ?
          AND id_persona = ?
          AND id_departamento = ?
          AND id_tipo_discriminacion = ?
          AND (
                (id_grupo_etnico = ?)
                OR (id_grupo_etnico IS NULL AND ? IS NULL)
              )
          AND (
                (id_comunidad_linguistica = ?)
                OR (id_comunidad_linguistica IS NULL AND ? IS NULL)
              )
          AND id_fuente_dato = ?
    """, (
        fecha_id,
        persona_id,
        departamento_id,
        tipo_discriminacion_id,
        grupo_etnico_id, grupo_etnico_id,
        comunidad_linguistica_id, comunidad_linguistica_id,
        fuente_id
    ))
    return repo.fetch_one() is not None


def insert_caso_discriminacion(
    repo: FirebirdRepository,
    fecha_id: int,
    persona_id: int,
    departamento_id: int | None,
    tipo_discriminacion_id: int | None,
    grupo_etnico_id: int | None,
    comunidad_linguistica_id: int | None,
    fuente_id: int
):
    repo.execute("""
        INSERT INTO caso_discriminacion (
            id_fecha_aproximada,
            id_persona,
            id_departamento,
            id_tipo_discriminacion,
            id_grupo_etnico,
            id_comunidad_linguistica,
            id_fuente_dato
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        fecha_id,
        persona_id,
        departamento_id,
        tipo_discriminacion_id,
        grupo_etnico_id,
        comunidad_linguistica_id,
        fuente_id
    ))


def should_skip_row(first_col) -> bool:
    if pd.isna(first_col):
        return True

    val = normalize_text(first_col)

    if val in {"", "total", "nº", "no"}:
        return True

    return False


def build_sheet_dataframe(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Lee cada hoja con header manual en 3.
    """
    return pd.read_excel(file_path, sheet_name=sheet_name, header=3)


def process_old_format(df: pd.DataFrame, year: int) -> list[dict]:
    """
    Formato 2016-2019
    Columnas esperadas:
    0 nro
    1 sexo_1
    2 sexo_2
    3 edad
    4 departamento
    5 tipo_discriminacion
    6 grupo_etnico
    7 comunidad_linguistica
    """
    records = []

    for _, row in df.iterrows():
        if should_skip_row(row.iloc[0]):
            continue

        sexo = parse_sexo_old(row)
        edad = parse_edad(row.iloc[3])
        departamento = normalize_departamento(row.iloc[4])
        tipo_discriminacion = normalize_tipo_discriminacion(row.iloc[5])
        grupo_etnico = extract_grupo_etnico_old(row)
        comunidad = normalize_comunidad(row.iloc[7])

        if not departamento and not tipo_discriminacion and not grupo_etnico and not comunidad:
            continue

        records.append({
            "anio": year,
            "sexo": sexo,
            "edad": edad,
            "departamento": departamento,
            "tipo_discriminacion": tipo_discriminacion,
            "grupo_etnico": grupo_etnico,
            "comunidad_linguistica": comunidad,
        })

    return records


def process_new_format(df: pd.DataFrame, year: int) -> list[dict]:
    """
    Formato 2020-2023
    0 nro
    1 sexo_m
    2 sexo_h
    3 edad
    4 maya
    5 garifuna
    6 xinka
    7 comunidad
    8 departamento
    9 tipo_discriminacion
    """
    records = []

    for _, row in df.iterrows():
        if should_skip_row(row.iloc[0]):
            continue

        sexo = parse_sexo_new(row)
        edad = parse_edad(row.iloc[3])
        grupo_etnico = extract_grupo_etnico_new(row)
        comunidad = normalize_comunidad(row.iloc[7])
        departamento = normalize_departamento(row.iloc[8])
        tipo_discriminacion = normalize_tipo_discriminacion(row.iloc[9])

        if not departamento and not tipo_discriminacion and not grupo_etnico and not comunidad:
            continue

        records.append({
            "anio": year,
            "sexo": sexo,
            "edad": edad,
            "departamento": departamento,
            "tipo_discriminacion": tipo_discriminacion,
            "grupo_etnico": grupo_etnico,
            "comunidad_linguistica": comunidad,
        })

    return records


def build_all_records(file_path: str) -> list[dict]:
    all_records = []

    for sheet in SHEETS:
        year = int(sheet)
        print(f"Procesando hoja: {sheet}")

        df = build_sheet_dataframe(file_path, sheet)

        if year <= 2019:
            records = process_old_format(df, year)
        else:
            records = process_new_format(df, year)

        print(f"  Registros útiles detectados: {len(records)}")
        all_records.extend(records)

    return all_records


def run_discriminacion_etl(repo: FirebirdRepository):
    if not Path(FILE_PATH).exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_PATH}")

    print(f"Procesando archivo: {FILE_PATH}")

    fuente_id = get_or_create_fuente_dato(repo)
    dep_map = build_departamento_map(repo)
    records = build_all_records(FILE_PATH)

    inserted = 0
    skipped_missing_depto = 0
    skipped_missing_fecha = 0
    skipped_missing_sexo = 0
    skipped_invalid = 0

    for rec in records:
        fecha_id = get_fecha_id(repo, rec["anio"])
        if not fecha_id:
            print(f"No existe fecha para año {rec['anio']}")
            skipped_missing_fecha += 1
            continue

        departamento_id = None

        if not rec["departamento"]:
            skipped_missing_depto += 1
            continue

        departamento_id = dep_map.get(rec["departamento"])
        if not departamento_id:
            print(f"Departamento no encontrado: {rec['departamento']} ({rec['anio']})")
            skipped_missing_depto += 1
            continue

        tipo_discriminacion_id = None
        if rec["tipo_discriminacion"]:
            tipo_discriminacion_id = get_or_create_tipo_discriminacion(
                repo,
                rec["tipo_discriminacion"]
            )

        grupo_etnico_id = get_or_create_grupo_etnico(repo, rec["grupo_etnico"])
        comunidad_id = get_or_create_comunidad_linguistica(repo, rec["comunidad_linguistica"])

        sexo_id = None
        if rec["sexo"]:
            sexo_id = get_or_create_sexo(repo, rec["sexo"])

        if sexo_id is None:
            skipped_missing_sexo += 1
            continue

        if rec["edad"] is None and not tipo_discriminacion_id and not grupo_etnico_id and not comunidad_id:
            skipped_invalid += 1
            continue

        persona_id = create_persona(repo, sexo_id, rec["edad"])

        insert_caso_discriminacion(
            repo,
            fecha_id=fecha_id,
            persona_id=persona_id,
            departamento_id=departamento_id,
            tipo_discriminacion_id=tipo_discriminacion_id,
            grupo_etnico_id=grupo_etnico_id,
            comunidad_linguistica_id=comunidad_id,
            fuente_id=fuente_id
        )
        inserted += 1

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos por departamento no encontrado: {skipped_missing_depto}")
    print(f"Omitidos por fecha faltante: {skipped_missing_fecha}")
    print(f"Omitidos por sexo faltante: {skipped_missing_sexo}")
    print(f"Omitidos por registro inválido: {skipped_invalid}")