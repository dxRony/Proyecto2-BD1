from datetime import date
from pathlib import Path
import hashlib
import unicodedata

import pandas as pd

from repositories.firebird_repository import FirebirdRepository

# diccionarios para normalizacion de meses y dias en español
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

#metodo para renombrar las columnas del dataframe a nombres canónicos esperados, basándose en el orden de las columnas
def canonicalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "departamento",
        "municipio",
        "anio",
        "delito",
        "estado_caso",
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

#metodo para obtener o crear una fuente de dati
def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("MP", dataset_name, "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("MP", dataset_name, "Excel"))
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

#metodo para obtener o crear un departamento "Ignorado", devolviendo su id
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

#metodo para obtener o crear un municipio "Ignorado", devolviendo su id el municipio se asocia al departamento "Ignorado"
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

#metodo para construir un mapa de nombres normalizados de municipios a sus ids, asegurando que exista el departamento "Ignorado"
def build_municipio_name_map(repo: FirebirdRepository) -> dict:
    get_or_create_municipio_ignorado(repo)

    repo.execute("""
        SELECT id, nombre
        FROM municipio
    """)
    rows = repo.fetch_all()

    result = {}
    for municipio_id, nombre in rows:
        result[normalize_name(nombre)] = municipio_id
    return result

#metodo para obtener o crear un estado de caso, devolviendo su id
def get_or_create_estado_caso(repo: FirebirdRepository, nombre: str) -> int:
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM estado_caso
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="EC", max_len=10)

    repo.execute("""
        INSERT INTO estado_caso (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]

#metodo para obtener o crear una categoria de delito, devolviendo su id
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

#metodo para obtener o crear un delito, devolviendo su id, asociando a una categoria de delito si se provee el nombre de la categoria
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


def insert_delito_contra_vida_mujer_estadistica(
    repo: FirebirdRepository,
    id_municipio: int,
    id_departamento: int,
    id_fecha: int,
    id_delito: int,
    id_estado_caso: int | None,
    cantidad: int
):
    repo.execute("""
        INSERT INTO delito_contra_vida_mujer_estadistica (
            id_municipio,
            id_departamento,
            id_fecha,
            id_delito,
            id_estado_caso,
            cantidad
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        id_municipio,
        id_departamento,
        id_fecha,
        id_delito,
        id_estado_caso,
        cantidad
    ))

#metodo para construir un dataframe agregado con las columnas necesarias para insertar en la tabla delito_contra_vida_mujer_estadistica, limpiando y normalizando los datos
def build_aggregated_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["departamento"] = df["departamento"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["municipio"] = df["municipio"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["anio"] = df["anio"].apply(safe_int)
    df["delito"] = df["delito"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["estado_caso"] = df["estado_caso"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["valor"] = df["valor"].apply(lambda x: safe_int(x) or 0)
    df = df[df["anio"].notna()].copy()
    df = df[df["valor"] > 0].copy()

    grouped = (
        df.groupby(
            ["departamento", "municipio", "anio", "delito", "estado_caso"],
            as_index=False
        )["valor"]
        .sum()
        .rename(columns={"valor": "cantidad"})
    )

    return grouped

#ejecutor etl
def run_delitos_vida_mujer_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Delitos contra la vida de las mujeres y femicidios del MP"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")
    #obtenemos el dataframe y normalizandolo
    df = pd.read_excel(file_path, sheet_name="2008-2024", header=0)
    df = canonicalize_dataframe_columns(df)
    df = build_aggregated_dataframe(df)
    #obtenemos o creamos la fuente de dato y los mapas de nombres a ids para departamentos y municipios
    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    departamento_name_map = build_departamento_name_map(repo)
    municipio_name_map = build_municipio_name_map(repo)

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_departamento = 0
    skipped_missing_municipio = 0
    skipped_missing_delito = 0
    skipped_missing_estado_caso = 0
    #recorriendo cada elemento del df
    for _, row in df.iterrows():
        anio = safe_int(row.get("anio"))
        if anio is None:
            skipped_missing_fecha += 1
            continue

        try:
            fecha_id = get_or_create_fecha(repo, anio, 1, 1)
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
        municipio_id = municipio_name_map.get(normalize_name(municipio_nombre))
        if not municipio_id:
            skipped_missing_municipio += 1
            municipio_id = municipio_name_map.get("ignorado")
        if not municipio_id:
            municipio_id = get_or_create_municipio_ignorado(repo)

        delito_nombre = clean_catalog_value(row.get("delito"), "Ignorado")
        if not delito_nombre:
            skipped_missing_delito += 1
            continue

        try:
            delito_id = get_or_create_delito(repo, delito_nombre, "Delitos contra la vida de las mujeres")
        except Exception:
            skipped_missing_delito += 1
            continue

        estado_caso_nombre = clean_catalog_value(row.get("estado_caso"), "Ignorado")
        try:
            estado_caso_id = get_or_create_estado_caso(repo, estado_caso_nombre)
        except Exception:
            skipped_missing_estado_caso += 1
            continue

        cantidad = safe_int(row.get("cantidad")) or 0
        if cantidad <= 0:
            continue
        #insertando el registro en la tabla delito_contra_vida_mujer_estadistica
        insert_delito_contra_vida_mujer_estadistica(
            repo=repo,
            id_municipio=municipio_id,
            id_departamento=departamento_id,
            id_fecha=fecha_id,
            id_delito=delito_id,
            id_estado_caso=estado_caso_id,
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
    print(f"Omitidos por municipio no encontrado: {skipped_missing_municipio}")
    print(f"Omitidos por delito inválido: {skipped_missing_delito}")
    print(f"Omitidos por estado de caso inválido: {skipped_missing_estado_caso}")