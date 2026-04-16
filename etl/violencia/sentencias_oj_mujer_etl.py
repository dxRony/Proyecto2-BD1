from datetime import date
from pathlib import Path
import hashlib
import unicodedata

import pandas as pd

from repositories.firebird_repository import FirebirdRepository

# diccionarios para normalizacion de meses y dias en español
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

#metodo para normalizar texto eliminando espacios
def normalize_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()

#metodo para normalizar texto eliminando acentos, caracteres especiales y convirtiendo a minusculas
def normalize_name(text) -> str:
    if text is None or pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = " ".join(text.split())
    return text

#metodo para convertir a entero de forma segura, devolviendo None si no se puede convertir o si el valor es considerado "ignorado"
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

#metodoa para generar codigo unico basado en el texto
def build_unique_code(text: str, prefix: str = "", max_len: int = 20) -> str:
    base = normalize_name(text).replace(" ", "_").upper()
    digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:3].upper()

    prefix = prefix.upper() if prefix else "X"

    reserve = len(prefix) + 1 + len(digest)
    cut_len = max_len - reserve
    if cut_len < 1:
        cut_len = 1

    trimmed = base[:cut_len]
    return f"{prefix}{trimmed}_{digest}"

#metodo para renombrar las columnas del dataframe a nombres canónicos esperados, basándose en el orden de las columnas
def canonicalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "departamento",
        "despacho",
        "delito",
        "tipo_fallo",
        "valor",
        "mes",
        "anio",
    ]

    if len(df.columns) < len(expected_columns):
        raise ValueError(
            f"Se esperaban al menos {len(expected_columns)} columnas, pero llegaron {len(df.columns)}"
        )

    rename_map = {}
    for idx, new_name in enumerate(expected_columns):
        rename_map[df.columns[idx]] = new_name

    return df.rename(columns=rename_map)

# metodo para limpiar y normalizar valores de catalogos "ignorado"
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

#metodo para parsear el nombre del mes en español a su numero correspondiente
def parse_mes_to_int(mes_texto: str):
    return MESES_ES.get(normalize_name(mes_texto))

#metodo para obtener o crear una fuente de dati
def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("OJ", dataset_name, "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("OJ", dataset_name, "Excel"))
    return repo.fetch_one()[0]

#metodo para obtener o crear una fecha, devolviendo su id
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

#metodo para obtener o crear un departamento "Ignorado" con codigo "9999"
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

#metodo para construir un mapa de nombres normalizados de departamentos a sus ids, asegurando que exista el departamento "Ignorado"
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

#metodo para obtener o crear un despacho, devolviendo su id
def get_or_create_despacho(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

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

#metodo para obtener o crear categoria_delito, retornando el id
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

    base_codigo = build_unique_code(nombre, prefix="C", max_len=20)
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
        codigo = base_codigo[:20 - len(suffix)] + suffix

    raise ValueError(f"No se pudo generar código único para categoría delito: {nombre}")

#metodo para obtener o crear tipo de delito, retornando el id
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
    base_codigo = build_unique_code(nombre, prefix="D", max_len=20)
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
        codigo = base_codigo[:20 - len(suffix)] + suffix

    raise ValueError(f"No se pudo generar código único para delito: {nombre}")

#metodo para obtener o crear un tipo_fallo, devolviendo su id
def get_or_create_tipo_fallo(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM tipo_fallo
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="TF", max_len=20)

    repo.execute("""
        INSERT INTO tipo_fallo (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]

#metodo para insertar sentencia oj estadistica
def insert_sentencia_oj_estadistica(
    repo: FirebirdRepository,
    id_departamento: int,
    id_despacho: int,
    id_delito: int,
    id_tipo_fallo: int,
    id_fecha: int,
    cantidad: int
):
    repo.execute("""
        INSERT INTO sentencias_oj_estadistica (
            id_departamento,
            id_despacho,
            id_delito,
            id_tipo_fallo,
            id_fecha,
            cantidad
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        id_departamento,
        id_despacho,
        id_delito,
        id_tipo_fallo,
        id_fecha,
        cantidad
    ))

#metodo para construir un dataframe agregado por fecha e indicador, sumando las cantidades
def build_aggregated_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["departamento"] = df["departamento"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["despacho"] = df["despacho"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["delito"] = df["delito"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["tipo_fallo"] = df["tipo_fallo"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["mes_num"] = df["mes"].apply(parse_mes_to_int)
    df["anio"] = df["anio"].apply(safe_int)
    df["valor"] = df["valor"].apply(lambda x: safe_int(x) or 0)

    df = df[df["anio"].notna()].copy()
    df = df[df["mes_num"].notna()].copy()
    df = df[df["valor"] > 0].copy()

    grouped = (
        df.groupby(
            ["departamento", "despacho", "delito", "tipo_fallo", "mes_num", "anio"],
            as_index=False
        )["valor"]
        .sum()
        .rename(columns={"valor": "cantidad"})
    )

    return grouped

#ejecutor etl
def run_sentencias_oj_vcm_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "SENTENCIAS DEL OJ POR EL DELITO DE VCM 2008-2024"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")
    # Cargar el archivo Excel y normalizar columnas
    df = pd.read_excel(file_path, sheet_name="Sentencias 2008-2024", header=0)
    df = canonicalize_dataframe_columns(df)
    df = build_aggregated_dataframe(df)
    #obteniendo llaves foraneas necesarias para las inserciones
    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    departamento_name_map = build_departamento_name_map(repo)

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_departamento = 0
    skipped_missing_despacho = 0
    skipped_missing_delito = 0
    skipped_missing_tipo_fallo = 0
    #recorriendo cada elemento en el df para insertar registros
    for _, row in df.iterrows():
        anio = safe_int(row.get("anio"))
        mes = safe_int(row.get("mes_num"))

        if anio is None or mes is None:
            skipped_missing_fecha += 1
            continue
        try:
            fecha_id = get_or_create_fecha(repo, anio, mes, 1)
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

        despacho_nombre = clean_catalog_value(row.get("despacho"), "Ignorado")
        try:
            despacho_id = get_or_create_despacho(repo, despacho_nombre)
        except Exception:
            skipped_missing_despacho += 1
            continue

        delito_nombre = clean_catalog_value(row.get("delito"), "Ignorado")
        try:
            delito_id = get_or_create_delito(repo, delito_nombre, "Sentencias OJ VCM")
        except Exception:
            skipped_missing_delito += 1
            continue

        tipo_fallo_nombre = clean_catalog_value(row.get("tipo_fallo"), "Ignorado")
        try:
            tipo_fallo_id = get_or_create_tipo_fallo(repo, tipo_fallo_nombre)
        except Exception:
            skipped_missing_tipo_fallo += 1
            continue

        cantidad = safe_int(row.get("cantidad")) or 0
        if cantidad <= 0:
            continue

        insert_sentencia_oj_estadistica(
            repo=repo,
            id_departamento=departamento_id,
            id_despacho=despacho_id,
            id_delito=delito_id,
            id_tipo_fallo=tipo_fallo_id,
            id_fecha=fecha_id,
            cantidad=cantidad
        )

        inserted += 1

        if inserted % 1000 == 0:
            print(f"Procesados correctamente: {inserted}")

    repo.commit()

    print(f"Fuente de dato usada: {fuente_id}")
    print(f"Insertados: {inserted}")
    print(f"Omitidos por fecha inválida: {skipped_missing_fecha}")
    print(f"Omitidos por departamento no encontrado: {skipped_missing_departamento}")
    print(f"Omitidos por despacho inválido: {skipped_missing_despacho}")
    print(f"Omitidos por delito inválido: {skipped_missing_delito}")
    print(f"Omitidos por tipo de fallo inválido: {skipped_missing_tipo_fallo}")