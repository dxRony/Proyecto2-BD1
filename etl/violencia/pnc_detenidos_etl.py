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
        if normalize_name(text) in {"ignorada", "ignorado", "nan", ""}:
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
        "anio_ocu",
        "mes_ocu",
        "dia_ocu",
        "dia_sem_ocu",
        "hora_ocu",
        "franja_horaria",
        "franja_horaria_alt",
        "area_geografica",
        "depto_ocu",
        "municipio_ocu",
        "col_extra",
        "sexo_per",
        "edad_per",
        "g_edad_60ymas",
        "g_edad_80ymas",
        "edad_quinquenales",
        "delito_com",
        "g_delitos",
    ]

    if len(df.columns) < len(expected_columns):
        raise ValueError(
            f"Se esperaban al menos {len(expected_columns)} columnas, pero llegaron {len(df.columns)}"
        )

    rename_map = {}
    for idx, new_name in enumerate(expected_columns):
        rename_map[df.columns[idx]] = new_name

    return df.rename(columns=rename_map)


def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("PNC", dataset_name, "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("PNC", dataset_name, "Excel"))

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


def build_municipio_name_map(repo: FirebirdRepository) -> dict:
    repo.execute("""
        SELECT id, nombre
        FROM municipio
    """)
    rows = repo.fetch_all()

    result = {}
    for municipio_id, nombre in rows:
        result[normalize_name(nombre)] = municipio_id

    return result


def get_or_create_sexo(repo: FirebirdRepository, nombre: str) -> int:
    nombre_norm = normalize_name(nombre)

    if nombre_norm in {"hombre", "hombres", "masculino"}:
        codigo = "H"
        nombre_final = "Hombre"
    elif nombre_norm in {"mujer", "mujeres", "femenino"}:
        codigo = "M"
        nombre_final = "Mujer"
    else:
        raise ValueError(f"Sexo no reconocido: {nombre}")

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


def get_or_create_area_geografica(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM area_geografica
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO area_geografica (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]


def parse_franja_range(nombre: str):
    nombre = normalize_text(nombre)
    if not nombre:
        return None, None

    nombre_norm = normalize_name(nombre)
    if nombre_norm in {"ignorada", "ignorado"}:
        return None, None

    if " a " in nombre:
        partes = nombre.split(" a ")
        if len(partes) == 2:
            hora_inicio = partes[0].strip()
            hora_fin = partes[1].strip()
            if len(hora_inicio) == 5 and len(hora_fin) == 5:
                return hora_inicio, hora_fin

    return None, None


def get_or_create_franja_horaria(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    nombre_norm = normalize_name(nombre)
    if nombre_norm in {"ignorada", "ignorado"}:
        return None

    repo.execute("""
        SELECT id
        FROM franja_horaria
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="F", max_len=10)
    hora_inicio, hora_fin = parse_franja_range(nombre)

    if not hora_inicio or not hora_fin:
        return None

    repo.execute("""
        INSERT INTO franja_horaria (codigo, nombre, hora_inicio, hora_fin)
        VALUES (?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, hora_inicio, hora_fin))
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
    """, (codigo, nombre, None, None, "PNC detenidos"))
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

    codigo = build_unique_code(nombre, prefix="C", max_len=10)

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
    codigo = build_unique_code(nombre, prefix="D", max_len=10)

    repo.execute("""
        INSERT INTO delito (codigo, nombre, id_capitulo, id_categoria_delito)
        VALUES (?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, None, id_categoria))
    return repo.fetch_one()[0]


def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]


def create_hecho_delictivo(
    repo: FirebirdRepository,
    id_fecha: int,
    id_municipio: int,
    id_delito: int,
    id_area_geografica: int | None,
    id_franja_horaria: int | None
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


def get_or_create_involucramiento(repo: FirebirdRepository, nombre: str = "Detenido") -> int:
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


def parse_mes_to_int(mes_texto: str):
    return MESES_ES.get(normalize_name(mes_texto))


def run_pnc_detenidos_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "PNC Detenidos"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")

    df = pd.read_excel(file_path, sheet_name="Sheet1", header=0)
    df = canonicalize_dataframe_columns(df)

    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    municipio_name_map = build_municipio_name_map(repo)
    involucramiento_detenido_id = get_or_create_involucramiento(repo, "Detenido")

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_municipio = 0
    skipped_missing_sexo = 0
    skipped_missing_delito = 0

    for _, row in df.iterrows():
        anio = safe_int(row.get("anio_ocu"))
        mes = parse_mes_to_int(row.get("mes_ocu"))
        dia = safe_int(row.get("dia_ocu"))

        if anio is None or mes is None or dia is None:
            skipped_missing_fecha += 1
            continue

        try:
            fecha_id = get_or_create_fecha(repo, anio, mes, dia)
        except Exception:
            skipped_missing_fecha += 1
            continue

        municipio_nombre = normalize_text(row.get("municipio_ocu"))
        municipio_id = municipio_name_map.get(normalize_name(municipio_nombre))
        if not municipio_id:
            skipped_missing_municipio += 1
            continue

        sexo_nombre = normalize_text(row.get("sexo_per"))
        if not sexo_nombre or normalize_name(sexo_nombre) in {"ignorada", "ignorado"}:
            skipped_missing_sexo += 1
            continue

        try:
            sexo_id = get_or_create_sexo(repo, sexo_nombre)
        except Exception:
            skipped_missing_sexo += 1
            continue

        edad = safe_int(row.get("edad_per"))

        grupo_etario_nombre = normalize_text(row.get("edad_quinquenales"))
        if normalize_name(grupo_etario_nombre) in {"ignorado", "ignorada"}:
            grupo_etario_nombre = ""
        grupo_etario_id = get_or_create_grupo_etario(repo, grupo_etario_nombre) if grupo_etario_nombre else None

        delito_nombre = normalize_text(row.get("delito_com"))
        categoria_delito_nombre = normalize_text(row.get("g_delitos"))

        if not delito_nombre:
            skipped_missing_delito += 1
            continue

        delito_id = get_or_create_delito(repo, delito_nombre, categoria_delito_nombre)

        area_geografica_nombre = normalize_text(row.get("area_geografica"))
        area_geografica_id = get_or_create_area_geografica(repo, area_geografica_nombre) if area_geografica_nombre else None

        franja_nombre = normalize_text(row.get("franja_horaria"))
        franja_id = get_or_create_franja_horaria(repo, franja_nombre) if franja_nombre else None

        persona_id = create_persona(repo, sexo_id, edad)

        hecho_id = create_hecho_delictivo(
            repo=repo,
            id_fecha=fecha_id,
            id_municipio=municipio_id,
            id_delito=delito_id,
            id_area_geografica=area_geografica_id,
            id_franja_horaria=franja_id
        )

        insert_involucramiento_hecho(
            repo=repo,
            id_persona=persona_id,
            id_hecho_delictivo=hecho_id,
            id_involucramiento=involucramiento_detenido_id,
            id_grupo_etario=grupo_etario_id,
            id_fuente_dato=fuente_id
        )

        inserted += 1

        if inserted % 1000 == 0:
            print(f"Procesados correctamente: {inserted}")

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos por fecha inválida: {skipped_missing_fecha}")
    print(f"Omitidos por municipio no encontrado: {skipped_missing_municipio}")
    print(f"Omitidos por sexo no reconocido: {skipped_missing_sexo}")
    print(f"Omitidos por delito faltante: {skipped_missing_delito}")