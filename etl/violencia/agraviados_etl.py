from datetime import date
from pathlib import Path
import hashlib
import unicodedata

import pandas as pd

from repositories.firebird_repository import FirebirdRepository

# Diccionarios para normalizacion de meses y dias en español
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

# metodo para limpiar y normalizar valores de catalogos "ignorado"
def clean_catalog_value(value: str, default: str = "Ignorado") -> str:
    text = normalize_text(value)
    norm = normalize_name(text)

    if norm in {"", "sd", "s/d", "ignorado", "ignorada", "999", "9999", "99", "nan"}:
        return default

    return text

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
        "anio_denuncia",
        "mes_denuncia",
        "dia_denuncia",
        "col_extra_1",
        "anio_nacimiento",
        "mes_nacimiento",
        "dia_nacimiento",
        "dia_sem_nacimiento",
        "depto_denuncia",
        "municipio_denuncia",
        "col_extra_2",
        "col_extra_3",
        "sexo_per",
        "g_edad_60ymas",
        "g_edad_80ymas",
        "edad_quinquenales",
        "estado_civil",
        "delito_com",
        "principales_delitos",
        "hora_denuncia",
        "g_hora_denuncia",
        "g_hora_denuncia_man_tar_noc",
    ]

    if len(df.columns) < len(expected_columns):
        raise ValueError(
            f"Se esperaban al menos {len(expected_columns)} columnas, pero llegaron {len(df.columns)}"
        )

    rename_map = {}
    for idx, new_name in enumerate(expected_columns):
        rename_map[df.columns[idx]] = new_name

    return df.rename(columns=rename_map)

#metodo para convertir el mes a su numero correspondiente
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

#metodo para construir un mapa de nombres normalizados de municipios a sus ids, asegurando que exista un municipio "Ignorado"
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

#metodo para obtener o crear estado conyugal, retornando el id
def get_or_create_estado_conyugal(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM estado_conyugal
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="EC", max_len=10)

    repo.execute("""
        INSERT INTO estado_conyugal (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
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
    """, (codigo, nombre, None, None, "Agraviados"))
    return repo.fetch_one()[0]

#metodo para parsear un rango de franja horaria, devolviendo hora_inicio y hora_fin en formato "HH:MM", o None si no se puede parsear
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

#metodo apra obtener o crear una franja horaria
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

#metodo para obtener o crear una categoria de delito, retornando el id
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
        codigo = (base_codigo[:10 - len(suffix)] + suffix)

    raise ValueError(f"No se pudo generar codigo único para categoria delito: {nombre}")

#metodo para obtener o crear un delito, retornando el id, y asociando a una categoria de delito si exite
def get_or_create_delito(repo: FirebirdRepository, nombre: str, categoria_nombre: str | None):
    #intentando obtener
    repo.execute("""
        SELECT id
        FROM delito
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]
    #creando categoria de delito si hay un nombre
    id_categoria = get_or_create_categoria_delito(repo, categoria_nombre) if categoria_nombre else None

    base_codigo = build_unique_code(nombre, prefix="D", max_len=10)
    codigo = base_codigo
    #intentando crear un codigo unico del delito
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
        codigo = (base_codigo[:10 - len(suffix)] + suffix)

    raise ValueError(f"No se pudo generar codigo unico para delito: {nombre}")

#metodo para crear una persona, devolviendo su id
def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None = None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]

#metodo para obtener o crear un detalle_persona asociado a una persona
def get_or_create_detalle_persona(
    repo: FirebirdRepository,
    id_persona: int,
    id_estado_conyugal: int | None
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
        id_estado_conyugal,
        None,
        None,
        None,
        None,
        None
    ))

#metodo para crear un hecho delictivo, devolviendo su id
def create_hecho_delictivo(
    repo: FirebirdRepository,
    id_fecha: int,
    id_municipio: int,
    id_delito: int,
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
        None,
        id_franja_horaria
    ))
    return repo.fetch_one()[0]

#metodo para obtener o crear un involucramiento, retornando su id
def get_or_create_involucramiento(repo: FirebirdRepository, nombre: str = "Agraviado") -> int:
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

#metodp para insertar un involucramiento_hecho asociando una persona con un hecho delictivo, su tipo de involucramiento, grupo etario y fuente de dato
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

#ejecutor etl
def run_agraviados_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Agraviados MP"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")
    #leyendo el excel y normalizando columnas
    df = pd.read_excel(file_path, sheet_name="Sheet1", header=0)
    df = canonicalize_dataframe_columns(df)
    #creando llaves foraneas necesarias
    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    municipio_name_map = build_municipio_name_map(repo)
    involucramiento_id = get_or_create_involucramiento(repo, "Agraviado")

    inserted = 0
    skipped_missing_fecha = 0
    skipped_missing_municipio = 0
    skipped_missing_sexo = 0
    skipped_missing_delito = 0
    #recorriendo cada fila del dataframe para procesar e insertar en la base de datos
    for _, row in df.iterrows():
        anio = safe_int(row.get("anio_denuncia"))
        mes = parse_mes_to_int(row.get("mes_denuncia"))
        dia = safe_int(row.get("dia_denuncia"))

        if anio is None or mes is None or dia is None:
            skipped_missing_fecha += 1
            continue

        try:
            fecha_id = get_or_create_fecha(repo, anio, mes, dia)
        except Exception:
            skipped_missing_fecha += 1
            continue

        municipio_nombre = normalize_text(row.get("municipio_denuncia"))
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

        sexo_nombre = clean_catalog_value(row.get("sexo_per"))
        if normalize_name(sexo_nombre) == "ignorado":
            skipped_missing_sexo += 1
        sexo_id = get_or_create_sexo(repo, sexo_nombre)

        estado_civil_nombre = clean_catalog_value(row.get("estado_civil"))
        if normalize_name(estado_civil_nombre) == "ignorado":
            estado_civil_nombre = ""
        estado_civil_id = get_or_create_estado_conyugal(repo, estado_civil_nombre) if estado_civil_nombre else None
        
        grupo_etario_nombre = clean_catalog_value(row.get("edad_quinquenales"))
        if normalize_name(grupo_etario_nombre) == "ignorado":
            grupo_etario_nombre = ""
        grupo_etario_id = get_or_create_grupo_etario(repo, grupo_etario_nombre) if grupo_etario_nombre else None

        delito_nombre = clean_catalog_value(row.get("delito_com"))
        categoria_delito_nombre = clean_catalog_value(row.get("principales_delitos"))
        if normalize_name(categoria_delito_nombre) == "ignorado":
            categoria_delito_nombre = None
        if normalize_name(delito_nombre) == "ignorado":
            skipped_missing_delito += 1
            continue
        delito_id = get_or_create_delito(repo, delito_nombre, categoria_delito_nombre)

        franja_nombre = clean_catalog_value(row.get("g_hora_denuncia"))
        if normalize_name(franja_nombre) == "ignorado":
            franja_nombre = clean_catalog_value(row.get("g_hora_denuncia_man_tar_noc"))
        if normalize_name(franja_nombre) == "ignorado":
            franja_nombre = ""
        franja_id = get_or_create_franja_horaria(repo, franja_nombre) if franja_nombre else None

        persona_id = create_persona(repo, sexo_id, None)
        get_or_create_detalle_persona(repo, persona_id, estado_civil_id)

        hecho_id = create_hecho_delictivo(
            repo=repo,
            id_fecha=fecha_id,
            id_municipio=municipio_id,
            id_delito=delito_id,
            id_franja_horaria=franja_id
        )

        insert_involucramiento_hecho(
            repo=repo,
            id_persona=persona_id,
            id_hecho_delictivo=hecho_id,
            id_involucramiento=involucramiento_id,
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