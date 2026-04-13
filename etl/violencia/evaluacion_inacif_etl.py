from datetime import date
from pathlib import Path
import hashlib
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


def normalize_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalize_name(text):
    if text is None or pd.isna(text):
        return ""
    return str(text).strip().lower()


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


def parse_mes(mes):
    return MESES_ES.get(normalize_name(mes))


def safe_int(val):
    try:
        if normalize_name(val) in {"ignorado", "ignorada", "nan", ""}:
            return None
        return int(float(val))
    except Exception:
        return None


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


def canonicalize(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "num",
        "anio",
        "mes",
        "dia",
        "dia_sem",
        "depto",
        "edad",
        "g60",
        "g80",
        "g_quin",
        "menor_mayor",
        "sexo",
        "tipo_eval"
    ]

    if len(df.columns) < len(cols):
        raise ValueError(
            f"Se esperaban al menos {len(cols)} columnas, pero llegaron {len(df.columns)}"
        )

    return df.rename(columns=dict(zip(df.columns, cols)))


def get_or_create_fuente_dato(
    repo: FirebirdRepository,
    institucion: str,
    dataset: str,
    tipo_fuente: str
) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, (institucion, dataset, tipo_fuente))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, (institucion, dataset, tipo_fuente))

    return repo.fetch_one()[0]


def get_or_create_sexo(repo: FirebirdRepository, nombre: str) -> int:
    nombre_norm = normalize_name(nombre)

    if nombre_norm in {"hombre", "masculino"}:
        codigo = "H"
        nombre_final = "Hombre"
    elif nombre_norm in {"mujer", "femenino"}:
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


def get_or_create_grupo_etario(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

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
    """, (codigo, nombre, None, None, "Evaluación INACIF"))
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

    base_codigo = build_unique_code(nombre, prefix="C", max_len=10)
    codigo = base_codigo

    for intento in range(10):
        repo.execute("""
            SELECT id
            FROM categoria_delito
            WHERE codigo = ?
        """, (codigo,))
        existing = repo.fetch_one()

        if not existing:
            repo.execute("""
                INSERT INTO categoria_delito (codigo, nombre)
                VALUES (?, ?)
                RETURNING id
            """, (codigo, nombre))
            return repo.fetch_one()[0]

        suffix = str(intento + 1)
        codigo = base_codigo[:10 - len(suffix)] + suffix

    raise ValueError(f"No se pudo generar código único para categoría delito: {nombre}")


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
    base_codigo = build_unique_code(nombre, prefix="D", max_len=10)
    codigo = base_codigo

    for intento in range(10):
        repo.execute("""
            SELECT id
            FROM delito
            WHERE codigo = ?
        """, (codigo,))
        existing = repo.fetch_one()

        if not existing:
            repo.execute("""
                INSERT INTO delito (codigo, nombre, id_capitulo, id_categoria_delito)
                VALUES (?, ?, ?, ?)
                RETURNING id
            """, (codigo, nombre, None, id_categoria))
            return repo.fetch_one()[0]

        suffix = str(intento + 1)
        codigo = base_codigo[:10 - len(suffix)] + suffix

    raise ValueError(f"No se pudo generar código único para delito: {nombre}")


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


def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None = None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
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


def get_or_create_detalle_persona(
    repo: FirebirdRepository,
    id_persona: int,
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
        None,
        id_condicion_edad,
        None,
        None,
        None
    ))


def create_hecho_delictivo(
    repo: FirebirdRepository,
    id_fecha: int,
    id_municipio: int,
    id_delito: int,
    id_area_geografica: int | None = None,
    id_franja_horaria: int | None = None
) -> int:
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
        id_municipio,
        None,
        id_delito,
        id_area_geografica,
        id_franja_horaria
    ))
    return repo.fetch_one()[0]


def get_or_create_involucramiento(repo: FirebirdRepository, nombre: str) -> int:
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


def insert_involucramiento_hecho(
    repo: FirebirdRepository,
    id_persona: int,
    id_hecho_delictivo: int,
    id_involucramiento: int,
    id_grupo_etario: int | None,
    id_fuente_dato: int
):
    repo.execute("""
        INSERT INTO involucramiento_hecho (
            id_persona,
            id_hecho_delictivo,
            id_involucramiento,
            id_grupo_etario,
            id_fuente_dato
        )
        VALUES (?, ?, ?, ?, ?)
    """, (
        id_persona,
        id_hecho_delictivo,
        id_involucramiento,
        id_grupo_etario,
        id_fuente_dato
    ))


def run_inacif_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Evaluaciones Médicas INACIF"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(file_path)

    print(f"Procesando archivo: {file_path}")

    df = pd.read_excel(file_path, sheet_name="Sheet1", header=0)
    df = canonicalize(df)

    fuente_id = get_or_create_fuente_dato(repo, "INACIF", dataset_name, "Excel")
    municipio_id_ignorado = get_or_create_municipio_ignorado(repo)
    involucramiento_id = get_or_create_involucramiento(repo, "Evaluado")

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_tipo_eval = 0

    for _, row in df.iterrows():
        anio = safe_int(row["anio"])
        mes = parse_mes(row["mes"])
        dia = safe_int(row["dia"])

        if not anio or not mes or not dia:
            skipped_missing_fecha += 1
            continue

        fecha_id = get_or_create_fecha(repo, anio, mes, dia)

        sexo = normalize_text(row["sexo"])
        sexo_id = get_or_create_sexo(repo, sexo)

        edad = safe_int(row["edad"])
        persona_id = create_persona(repo, sexo_id, edad)

        grupo = normalize_text(row["g_quin"])
        if normalize_name(grupo) in {"ignorado", "ignorada"}:
            grupo = None
        grupo_id = get_or_create_grupo_etario(repo, grupo) if grupo else None

        condicion_edad_nombre = normalize_text(row["menor_mayor"])
        if normalize_name(condicion_edad_nombre) in {"ignorado", "ignorada"}:
            condicion_edad_nombre = ""
        condicion_edad_id = get_or_create_condicion_edad(repo, condicion_edad_nombre) if condicion_edad_nombre else None
        get_or_create_detalle_persona(repo, persona_id, condicion_edad_id)

        tipo_eval = normalize_text(row["tipo_eval"])
        if not tipo_eval:
            skipped_missing_tipo_eval += 1
            continue

        delito_id = get_or_create_delito(repo, tipo_eval, "Evaluación INACIF")

        hecho_id = create_hecho_delictivo(
            repo,
            fecha_id,
            municipio_id_ignorado,
            delito_id,
            None,
            None
        )

        insert_involucramiento_hecho(
            repo,
            persona_id,
            hecho_id,
            involucramiento_id,
            grupo_id,
            fuente_id
        )

        inserted += 1

        if inserted % 1000 == 0:
            print(f"Procesados: {inserted}")

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos por fecha inválida: {skipped_missing_fecha}")
    print(f"Omitidos por tipo de evaluación faltante: {skipped_missing_tipo_eval}")