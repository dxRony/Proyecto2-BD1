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
    text = text.replace("(a)", "").replace("(o)", "")
    text = " ".join(text.split())
    return text

#metodo para convertir a entero de forma segura, devolviendo None si no se puede convertir o si el valor es considerado "ignorado"
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

#metodo para obtener o crear una fuente de dati
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

#metodo para construir un mapa de nombres normalizados de municipios a sus ids, usando tanto el nombre solo como el nombre combinado con el id del departamento, asegurando que exista el municipio "Ignorado"
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

#metodo para obtener o crear sexo, retornando el id
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

#metodo apar limpiar el valor de la edad
def clean_edad(value):
    edad = safe_int(value)
    if edad is None:
        return None

    if edad in {99, 999, 9999}:
        return None

    if edad < 0 or edad > 120:
        return None

    return edad

#metodo para obtener o crear area_geografica, retornando el id
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

#metodo para parsear un rango de franja horaria en formato "HH:MM a HH:MM", devolviendo hora_inicio y hora_fin, o None
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

#metodo para obtener o crear franja_horaria, retornando el id
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

#metodo para obtener o crear grupo etario, retornando el id
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

    codigo = build_unique_code(nombre, prefix="C", max_len=10)

    repo.execute("""
        INSERT INTO categoria_delito (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]

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
    codigo = build_unique_code(nombre, prefix="D", max_len=10)

    repo.execute("""
        INSERT INTO delito (codigo, nombre, id_capitulo, id_categoria_delito)
        VALUES (?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, None, id_categoria))
    return repo.fetch_one()[0]

# metodo para limpiar y normalizar valores de catalogos "ignorado"
def clean_catalog_value(value: str, default: str = "Ignorado") -> str:
    text = normalize_text(value)
    norm = normalize_name(text)

    if norm in {"", "sd", "s/d", "ignorado", "ignorada", "999", "9999", "99", "nan"}:
        return default

    return text

#metodo para crear una persona, devolviendo su id
def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]

#metodo para crear un hecho_delictivo, devolviendo su id
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

#metodo para obtener o crear involucramiento, retornando el id
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

#metodo para crear involucramiento, retornando el id
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

#metodo para parsear el nombre del mes en español a su numero correspondiente
def parse_mes_to_int(mes_texto: str):
    return MESES_ES.get(normalize_name(mes_texto))

#ejecutor etl
def run_pnc_detenidos_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "PNC Detenidos"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")
    # Cargar el archivo Excel y normalizar columnas
    df = pd.read_excel(file_path, sheet_name="Sheet1", header=0)
    df = canonicalize_dataframe_columns(df)
    #obteniendo llaves foraneas necesarias para las inserciones
    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    municipio_name_map = build_municipio_name_map(repo)
    involucramiento_detenido_id = get_or_create_involucramiento(repo, "Detenido")

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_municipio = 0
    skipped_missing_sexo = 0
    skipped_missing_delito = 0
    #recorriendo cada elemento en el df para insertar registros
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
        municipio_norm = normalize_name(municipio_nombre)
        if municipio_norm in {"", "ignorado", "ignorada", "9999", "999", "sd", "s/d"}:
            skipped_missing_municipio += 1
            municipio_id = municipio_name_map.get("ignorado")
        else:
            municipio_id = municipio_name_map.get(municipio_norm)
            if not municipio_id:
                skipped_missing_municipio += 1
                municipio_id = municipio_name_map.get("ignorado")
        if not municipio_id:
            municipio_id = get_or_create_municipio_ignorado(repo)

        sexo_nombre = normalize_text(row.get("sexo_per"))
        if normalize_name(sexo_nombre) in {"", "ignorada", "ignorado", "sd", "s/d", "999", "9"}:
            skipped_missing_sexo += 1
            sexo_nombre = "Ignorado"
        sexo_id = get_or_create_sexo(repo, sexo_nombre)

        edad = clean_edad(row.get("edad_per"))
        
        grupo_etario_nombre = clean_catalog_value(row.get("edad_quinquenales"))
        if normalize_name(grupo_etario_nombre) == "ignorado":
            grupo_etario_nombre = ""
        grupo_etario_id = get_or_create_grupo_etario(repo, grupo_etario_nombre) if grupo_etario_nombre else None

        delito_nombre = clean_catalog_value(row.get("delito_com"))
        categoria_delito_nombre = clean_catalog_value(row.get("g_delitos"))
        if normalize_name(categoria_delito_nombre) == "ignorado":
            categoria_delito_nombre = None
        if not delito_nombre or normalize_name(delito_nombre) == "ignorado":
            skipped_missing_delito += 1
            continue

        delito_id = get_or_create_delito(repo, delito_nombre, categoria_delito_nombre)

        area_geografica_nombre = clean_catalog_value(row.get("area_geografica"))
        if normalize_name(area_geografica_nombre) == "ignorado":
            area_geografica_nombre = ""
        area_geografica_id = get_or_create_area_geografica(repo, area_geografica_nombre) if area_geografica_nombre else None

        franja_nombre = clean_catalog_value(row.get("franja_horaria"))
        if normalize_name(franja_nombre) == "ignorado":
            franja_nombre = ""
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