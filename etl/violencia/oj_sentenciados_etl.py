from datetime import date
from pathlib import Path
import hashlib
import unicodedata

import pandas as pd

from repositories.firebird_repository import FirebirdRepository


MESES_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

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

def clean_catalog_value(value: str, default: str = "Ignorado") -> str:
    text = normalize_text(value)
    norm = normalize_name(text)

    if norm in {"", "sd", "s/d", "ignorado", "ignorada", "999", "9999", "99", "nan"}:
        return default

    return text

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
    text = text.replace("(a)", "").replace("(o)", "")
    text = " ".join(text.split())
    return text


def safe_int(value):
    if value is None or pd.isna(value):
        return None
    try:
        text = str(value).strip()
        if normalize_name(text) in {"ignorado", "ignorada", "nan", ""}:
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
        "num_corre",
        "anio_reg",
        "mes_reg",
        "men_may",
        "sexo",
        "nacionalidad",
        "involucramiento",
        "tipo_fallo",
        "departamento",
        "delito",
        "tipo_ley",
        "titulo",
        "capitulo",
    ]

    if len(df.columns) < len(expected_columns):
        raise ValueError(
            f"Se esperaban al menos {len(expected_columns)} columnas, pero llegaron {len(df.columns)}"
        )

    rename_map = {}
    for idx, new_name in enumerate(expected_columns):
        rename_map[df.columns[idx]] = new_name

    return df.rename(columns=rename_map)


def parse_mes_to_int(mes_texto: str):
    return MESES_ES.get(normalize_name(mes_texto))


def get_or_create_fecha(repo: FirebirdRepository, anio: int, mes: int, dia: int = 1):
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


def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("Organismo Judicial", dataset_name, "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("Organismo Judicial", dataset_name, "Excel"))
    return repo.fetch_one()[0]


def build_departamento_name_map(repo: FirebirdRepository) -> dict:
    get_or_create_departamento_ignorado(repo)

    repo.execute("""
        SELECT id, nombre
        FROM departamento
    """)
    rows = repo.fetch_all()

    result = {}
    for dep_id, nombre in rows:
        result[normalize_name(nombre)] = dep_id
    return result

def get_or_create_sexo(repo: FirebirdRepository, nombre: str) -> int:
    nombre_norm = normalize_name(nombre)

    if nombre_norm in {"hombre", "hombres", "masculino"}:
        codigo = "H"
        nombre_final = "Hombre"
    elif nombre_norm in {"mujer", "mujeres", "femenino"}:
        codigo = "M"
        nombre_final = "Mujer"
    elif nombre_norm in {"", "ignorado", "ignorada", "sd", "s/d", "999", "9"}:
        codigo = "9"
        nombre_final = "Ignorado"
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


def get_or_create_nacionalidad(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM nacionalidad
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="N", max_len=10)

    repo.execute("""
        INSERT INTO nacionalidad (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def get_or_create_condicion_edad(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM condicion_edad
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="E", max_len=10)

    repo.execute("""
        INSERT INTO condicion_edad (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def get_or_create_involucramiento(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM involucramiento
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="I", max_len=10)

    repo.execute("""
        INSERT INTO involucramiento (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def get_or_create_tipo_fallo(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM tipo_fallo
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="F", max_len=10)

    repo.execute("""
        INSERT INTO tipo_fallo (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def get_or_create_tipo_ley(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM tipo_ley
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="L", max_len=10)

    repo.execute("""
        INSERT INTO tipo_ley (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def get_or_create_titulo_ley(repo: FirebirdRepository, nombre: str, id_tipo_ley: int | None):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM titulo_ley
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="T", max_len=10)

    repo.execute("""
        INSERT INTO titulo_ley (codigo, nombre, id_tipo_ley)
        VALUES (?, ?, ?)
        RETURNING id
    """, (codigo, nombre, id_tipo_ley))
    return repo.fetch_one()[0]


def get_or_create_capitulo_ley(repo: FirebirdRepository, nombre: str, id_titulo: int | None):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM capitulo_ley
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="C", max_len=10)

    repo.execute("""
        INSERT INTO capitulo_ley (codigo, nombre, id_titulo)
        VALUES (?, ?, ?)
        RETURNING id
    """, (codigo, nombre, id_titulo))
    return repo.fetch_one()[0]


def get_or_create_categoria_delito(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM categoria_delito
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="D", max_len=10)

    repo.execute("""
        INSERT INTO categoria_delito (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def get_or_create_delito(repo: FirebirdRepository, nombre: str, categoria_nombre: str | None):
    repo.execute("""
        SELECT id
        FROM delito
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    id_categoria = get_or_create_categoria_delito(repo, categoria_nombre) if categoria_nombre else None
    codigo = build_unique_code(nombre, prefix="DL", max_len=10)

    repo.execute("""
        INSERT INTO delito (codigo, nombre, id_capitulo, id_categoria_delito)
        VALUES (?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, None, id_categoria))
    return repo.fetch_one()[0]


def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None = None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]


def get_or_create_detalle_persona(
    repo: FirebirdRepository,
    id_persona: int,
    id_nacionalidad: int | None,
    id_condicion_edad: int | None
):
    repo.execute("""
        SELECT id_persona
        FROM detalle_persona
        WHERE id_persona = ?
    """, (id_persona,))
    row = repo.fetch_one()
    if row:
        return

    repo.execute("""
        INSERT INTO detalle_persona (
            id_persona,
            id_estado_conyugal,
            id_nacionalidad,
            id_condicion_edad,
            id_escolaridad,
            id_grupo_etnico,
            id_orientacion
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        id_persona,
        None,
        id_nacionalidad,
        id_condicion_edad,
        None,
        None,
        None
    ))


def create_proceso_judicial(
    repo: FirebirdRepository,
    id_hecho_delictivo: int,
    id_departamento: int,
    id_delito: int
) -> int:
    repo.execute("""
        INSERT INTO proceso_judicial (
            id_hecho_delictivo,
            id_departamento,
            id_delito
        )
        VALUES (?, ?, ?)
        RETURNING id
    """, (
        id_hecho_delictivo,
        id_departamento,
        id_delito
    ))
    return repo.fetch_one()[0]


def create_sentencia(repo: FirebirdRepository, id_proceso_judicial: int, id_tipo_fallo: int):
    repo.execute("""
        INSERT INTO sentencia (
            id_proceso_judicial,
            id_tipo_fallo
        )
        VALUES (?, ?)
    """, (
        id_proceso_judicial,
        id_tipo_fallo
    ))


def create_hecho_placeholder(
    repo: FirebirdRepository,
    id_fecha: int,
    id_delito: int
) -> int:
    municipio_id = get_or_create_municipio_ignorado(repo)

    repo.execute("""
        INSERT INTO hecho_delictivo (
            id_fecha,
            id_municipio,
            id_zona,
            id_delito,
            id_area_geografica,
            id_franja_horaria
        )
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (
        id_fecha,
        municipio_id,
        None,
        id_delito,
        None,
        None
    ))
    return repo.fetch_one()[0]


def run_oj_sentenciados_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "OJ Sentenciados"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")

    df = pd.read_excel(file_path, sheet_name="Sheet1", header=0)
    df = canonicalize_dataframe_columns(df)

    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    dep_map = build_departamento_name_map(repo)

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_departamento = 0
    skipped_missing_sexo = 0
    skipped_missing_delito = 0
    skipped_missing_fallo = 0

    for _, row in df.iterrows():
        anio = safe_int(row.get("anio_reg"))
        mes = parse_mes_to_int(row.get("mes_reg"))

        if anio is None or mes is None:
            skipped_missing_fecha += 1
            continue

        try:
            fecha_id = get_or_create_fecha(repo, anio, mes, 1)
        except Exception:
            skipped_missing_fecha += 1
            continue

        departamento_nombre = clean_catalog_value(row.get("departamento"))
        departamento_norm = normalize_name(departamento_nombre)

        if departamento_norm == "ignorado":
            skipped_missing_departamento += 1
            departamento_id = dep_map.get("ignorado")
        else:
            departamento_id = dep_map.get(departamento_norm)
            if not departamento_id:
                skipped_missing_departamento += 1
                departamento_id = dep_map.get("ignorado")

        if not departamento_id:
            departamento_id = get_or_create_departamento_ignorado(repo)

        sexo_nombre = clean_catalog_value(row.get("sexo"))
        if normalize_name(sexo_nombre) == "ignorado":
            skipped_missing_sexo += 1

        sexo_id = get_or_create_sexo(repo, sexo_nombre)

        delito_nombre = clean_catalog_value(row.get("delito"))
        if normalize_name(delito_nombre) == "ignorado":
            skipped_missing_delito += 1
            continue

        tipo_fallo_nombre = clean_catalog_value(row.get("tipo_fallo"))
        if normalize_name(tipo_fallo_nombre) == "ignorado":
            skipped_missing_fallo += 1
            continue

        nacionalidad_nombre = clean_catalog_value(row.get("nacionalidad"))
        condicion_edad_nombre = clean_catalog_value(row.get("men_may"))
        involucramiento_nombre = clean_catalog_value(row.get("involucramiento"))
        tipo_ley_nombre = clean_catalog_value(row.get("tipo_ley"))
        titulo_nombre = clean_catalog_value(row.get("titulo"))
        capitulo_nombre = clean_catalog_value(row.get("capitulo"))

        if normalize_name(nacionalidad_nombre) == "ignorado":
            nacionalidad_nombre = ""
        if normalize_name(condicion_edad_nombre) == "ignorado":
            condicion_edad_nombre = ""
        if normalize_name(involucramiento_nombre) == "ignorado":
            involucramiento_nombre = ""
        if normalize_name(tipo_ley_nombre) == "ignorado":
            tipo_ley_nombre = ""
        if normalize_name(titulo_nombre) == "ignorado":
            titulo_nombre = ""
        if normalize_name(capitulo_nombre) == "ignorado":
            capitulo_nombre = ""
    
        id_nacionalidad = get_or_create_nacionalidad(repo, nacionalidad_nombre) if nacionalidad_nombre else None
        id_condicion_edad = get_or_create_condicion_edad(repo, condicion_edad_nombre) if condicion_edad_nombre else None
        get_or_create_involucramiento(repo, involucramiento_nombre) if involucramiento_nombre else None
        id_tipo_fallo = get_or_create_tipo_fallo(repo, tipo_fallo_nombre)
        id_tipo_ley = get_or_create_tipo_ley(repo, tipo_ley_nombre) if tipo_ley_nombre else None
        id_titulo = get_or_create_titulo_ley(repo, titulo_nombre, id_tipo_ley) if titulo_nombre else None
        get_or_create_capitulo_ley(repo, capitulo_nombre, id_titulo) if capitulo_nombre else None

        delito_id = get_or_create_delito(repo, delito_nombre, None)

        persona_id = create_persona(repo, sexo_id, None)
        get_or_create_detalle_persona(repo, persona_id, id_nacionalidad, id_condicion_edad)

        hecho_id = create_hecho_placeholder(repo, fecha_id, delito_id)
        proceso_id = create_proceso_judicial(repo, hecho_id, departamento_id, delito_id)
        create_sentencia(repo, proceso_id, id_tipo_fallo)

        inserted += 1

        if inserted % 1000 == 0:
            print(f"Procesados correctamente: {inserted}")

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos por fecha inválida: {skipped_missing_fecha}")
    print(f"Omitidos por departamento no encontrado: {skipped_missing_departamento}")
    print(f"Omitidos por sexo no reconocido: {skipped_missing_sexo}")
    print(f"Omitidos por delito faltante: {skipped_missing_delito}")
    print(f"Omitidos por tipo de fallo faltante: {skipped_missing_fallo}")