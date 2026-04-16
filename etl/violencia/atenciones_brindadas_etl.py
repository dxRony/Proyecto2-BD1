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
        "fecha_atencion",
        "dia",
        "mes",
        "anio",
        "sede_atencion",
        "departamento_sede",
        "municipio_sede",
        "tipo_delito_atendido",
        "atencion_brindada",
        "departamento_procedencia",
        "municipio_procedencia",
        "edad",
        "rango_edad",
        "grupo_etnico",
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
    """, ("Instituto de la Víctima Guatemala", dataset_name, "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("Instituto de la Víctima Guatemala", dataset_name, "Excel"))
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

#metodo para obtener o crear sexo, retornando el id
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

#metodo para obtener o crear un municipio "Ignorado" con codigo "M99999" asociado al departamento "Ignorado"
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

#metodo para construir un mapa de nombres normalizados de municipios a sus ids, usando tanto el nombre solo como el nombre combinado con el id del departamento, asegurando que exista el municipio "Ignorado"
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

#metodo para obtener o crear grupo etario, retornando el id
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
    """, (codigo, nombre, None, None, "Atencion victima mujer"))
    return repo.fetch_one()[0]

#metodo para obtener o crear grupo etnico, retornando el id
def get_or_create_grupo_etnico(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

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

#metodo para obtener o crear grupo orientacion, retornando el id
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

#metodo para obtener o crear tipo de atencion, retornando el id
def get_or_create_tipo_atencion(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM tipo_atencion
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO tipo_atencion (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]

#metodo para obtener o crear tipo de delito atendido, retornando el id
def get_or_create_tipo_delito_atendido(repo: FirebirdRepository, nombre: str):
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM tipo_delito_atendido
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO tipo_delito_atendido (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]

#metodo para obtener o crear sde, retornando el id
def get_or_create_sede(repo: FirebirdRepository, nombre: str, id_municipio: int) -> int:
    nombre = clean_catalog_value(nombre, "Ignorado")

    repo.execute("""
        SELECT id
        FROM sede
        WHERE LOWER(nombre) = LOWER(?)
          AND id_municipio = ?
    """, (nombre, id_municipio))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO sede (nombre, id_municipio)
        VALUES (?, ?)
        RETURNING id
    """, (nombre, id_municipio))
    return repo.fetch_one()[0]

#metodo para crear una persona, devolviendo su id
def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None = None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]

#metodo para insertar una atencion a victima, recibiendo los ids de las entidades relacionadas
def insert_atencion_victima(
    repo: FirebirdRepository,
    id_fecha: int,
    id_sede: int,
    id_tipo_delito_atendido: int | None,
    id_tipo_atencion: int | None,
    id_persona: int,
    id_municipio_origen: int | None,
    id_grupo_etario: int | None,
    id_grupo_etnico: int | None,
    id_orientacion: int | None,
    id_fuente_dato: int,
):
    repo.execute("""
        INSERT INTO atencion_victima (
            id_fecha,
            id_sede,
            id_tipo_delito_atendido,
            id_tipo_atencion,
            id_persona,
            id_municipio_origen,
            id_grupo_etario,
            id_grupo_etnico,
            id_orientacion,
            id_fuente_dato
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        id_fecha,
        id_sede,
        id_tipo_delito_atendido,
        id_tipo_atencion,
        id_persona,
        id_municipio_origen,
        id_grupo_etario,
        id_grupo_etnico,
        id_orientacion,
        id_fuente_dato
    ))

#metodo para construir un dataframe limpio y normalizado a partir del dataframe original, haciendo transformaciones necesarias para fecha, catalogos, edad y valor
def build_clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["fecha_atencion"] = pd.to_datetime(df["fecha_atencion"], errors="coerce")
    df["sede_atencion"] = df["sede_atencion"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["departamento_sede"] = df["departamento_sede"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["municipio_sede"] = df["municipio_sede"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["tipo_delito_atendido"] = df["tipo_delito_atendido"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["atencion_brindada"] = df["atencion_brindada"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["departamento_procedencia"] = df["departamento_procedencia"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["municipio_procedencia"] = df["municipio_procedencia"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["edad_num"] = df["edad"].apply(lambda x: safe_int(x))
    df["rango_edad"] = df["rango_edad"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["grupo_etnico"] = df["grupo_etnico"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["orientacion_sexual"] = df["orientacion_sexual"].apply(lambda x: clean_catalog_value(x, "Ignorado"))
    df["valor"] = df["valor"].apply(lambda x: safe_int(x) or 0)

    df = df[df["fecha_atencion"].notna()].copy()
    df = df[df["valor"] > 0].copy()

    return df

#ejecutro etl
def run_atenciones_victima_mujer_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Atenciones brindades por el Instituto de la Víctima 2020-2023"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")
    # Cargar el archivo Excel y normalizar columnas
    df = pd.read_excel(file_path, sheet_name="2020-2023", header=0)
    df = canonicalize_dataframe_columns(df)
    df = build_clean_dataframe(df)
    #obteniendo llaves foraneas necesarias para las inserciones
    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    departamento_name_map = build_departamento_name_map(repo)
    municipio_name_map = build_municipio_name_map(repo)
    sexo_id = get_or_create_sexo(repo, "Mujer")

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_sede = 0
    skipped_missing_municipio_sede = 0
    skipped_missing_municipio_origen = 0
    skipped_missing_tipo_atencion = 0
    skipped_missing_tipo_delito = 0
    #recorriendo cada elemento en el df para insertar atenciones brindadas
    for _, row in df.iterrows():
        fecha = row.get("fecha_atencion")
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
        #ubicacion de sede
        departamento_sede_nombre = clean_catalog_value(row.get("departamento_sede"), "Ignorado")
        departamento_sede_id = departamento_name_map.get(normalize_name(departamento_sede_nombre))
        if not departamento_sede_id:
            departamento_sede_id = departamento_name_map.get("ignorado")
        if not departamento_sede_id:
            departamento_sede_id = get_or_create_departamento_ignorado(repo)

        municipio_sede_nombre = clean_catalog_value(row.get("municipio_sede"), "Ignorado")
        municipio_sede_key = f"{normalize_name(municipio_sede_nombre)}|{departamento_sede_id}"
        municipio_sede_id = municipio_name_map.get(municipio_sede_key)
        if not municipio_sede_id:
            municipio_sede_id = municipio_name_map.get(normalize_name(municipio_sede_nombre))
        if not municipio_sede_id:
            skipped_missing_municipio_sede += 1
            municipio_sede_id = municipio_name_map.get("ignorado")
        if not municipio_sede_id:
            municipio_sede_id = get_or_create_municipio_ignorado(repo)
        sede_nombre = clean_catalog_value(row.get("sede_atencion"), "Ignorado")
        try:
            sede_id = get_or_create_sede(repo, sede_nombre, municipio_sede_id)
        except Exception:
            skipped_missing_sede += 1
            continue
        #ubicacion de origen
        departamento_origen_nombre = clean_catalog_value(row.get("departamento_procedencia"), "Ignorado")
        departamento_origen_id = departamento_name_map.get(normalize_name(departamento_origen_nombre))
        if not departamento_origen_id:
            departamento_origen_id = departamento_name_map.get("ignorado")
        if not departamento_origen_id:
            departamento_origen_id = get_or_create_departamento_ignorado(repo)
        municipio_origen_nombre = clean_catalog_value(row.get("municipio_procedencia"), "Ignorado")
        municipio_origen_key = f"{normalize_name(municipio_origen_nombre)}|{departamento_origen_id}"
        municipio_origen_id = municipio_name_map.get(municipio_origen_key)

        if not municipio_origen_id:
            municipio_origen_id = municipio_name_map.get(normalize_name(municipio_origen_nombre))

        if not municipio_origen_id:
            skipped_missing_municipio_origen += 1
            municipio_origen_id = municipio_name_map.get("ignorado")

        if not municipio_origen_id:
            municipio_origen_id = get_or_create_municipio_ignorado(repo)

        #catalogos de atencion
        tipo_delito_nombre = clean_catalog_value(row.get("tipo_delito_atendido"), "Ignorado")
        atencion_nombre = clean_catalog_value(row.get("atencion_brindada"), "Ignorado")
        grupo_etario_nombre = clean_catalog_value(row.get("rango_edad"), "Ignorado")
        grupo_etnico_nombre = clean_catalog_value(row.get("grupo_etnico"), "Ignorado")
        orientacion_nombre = clean_catalog_value(row.get("orientacion_sexual"), "Ignorado")

        try:
            tipo_delito_id = get_or_create_tipo_delito_atendido(repo, tipo_delito_nombre)
        except Exception:
            skipped_missing_tipo_delito += 1
            continue

        try:
            tipo_atencion_id = get_or_create_tipo_atencion(repo, atencion_nombre)
        except Exception:
            skipped_missing_tipo_atencion += 1
            continue

        try:
            grupo_etario_id = get_or_create_grupo_etario(repo, grupo_etario_nombre)
        except Exception:
            grupo_etario_id = None

        try:
            grupo_etnico_id = get_or_create_grupo_etnico(repo, grupo_etnico_nombre)
        except Exception:
            grupo_etnico_id = None

        try:
            orientacion_id = get_or_create_orientacion(repo, orientacion_nombre)
        except Exception:
            orientacion_id = None

        edad = row.get("edad_num")
        if pd.isna(edad):
            edad = None
        else:
            edad = safe_int(edad)

        cantidad = safe_int(row.get("valor")) or 0
        if cantidad <= 0:
            continue
        #recorriendo la cantidad de atenciones para insertar cada una como un registro individual, creando una persona por cada atencion
        for _ in range(cantidad):
            persona_id = create_persona(repo, sexo_id, edad)

            insert_atencion_victima(
                repo=repo,
                id_fecha=fecha_id,
                id_sede=sede_id,
                id_tipo_delito_atendido=tipo_delito_id,
                id_tipo_atencion=tipo_atencion_id,
                id_persona=persona_id,
                id_municipio_origen=municipio_origen_id,
                id_grupo_etario=grupo_etario_id,
                id_grupo_etnico=grupo_etnico_id,
                id_orientacion=orientacion_id,
                id_fuente_dato=fuente_id,
            )

            inserted += 1

            if inserted % 1000 == 0:
                print(f"Procesados correctamente: {inserted}")

    repo.commit()

    print(f"Fuente de dato usada: {fuente_id}")
    print(f"Insertados: {inserted}")
    print(f"Omitidos por fecha invalida: {skipped_missing_fecha}")
    print(f"Omitidos por sede invalida: {skipped_missing_sede}")
    print(f"Omitidos por municipio de sede no encontrado: {skipped_missing_municipio_sede}")
    print(f"Omitidos por municipio de origen no encontrado: {skipped_missing_municipio_origen}")
    print(f"Omitidos por tipo de atencion invalido: {skipped_missing_tipo_atencion}")
    print(f"Omitidos por tipo de delito atendido invalido: {skipped_missing_tipo_delito}")