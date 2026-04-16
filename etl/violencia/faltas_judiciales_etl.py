from datetime import date
from pathlib import Path
import unicodedata
import hashlib
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
    text = text.replace("(a)", "")
    text = text.replace("(o)", "")
    text = " ".join(text.split())
    return text

#metodo para convertir a entero de forma segura, devolviendo None si no se puede convertir o si el valor es considerado "ignorado"
def safe_int(value):
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except Exception:
        return None

#metodoa para generar codigo unico basado en el texto
def build_unique_code(text: str, prefix: str = "", max_len: int = 20) -> str:
    base = normalize_name(text).replace(" ", "_").upper()
    digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:6].upper()

    if prefix:
        prefix = prefix.upper() + "_"
    else:
        prefix = ""

    reserve = len(prefix) + 1 + len(digest)  # prefijo + "_" + hash
    cut_len = max_len - reserve

    if cut_len < 4:
        cut_len = 4

    trimmed = base[:cut_len]
    return f"{prefix}{trimmed}_{digest}"

#metodo para renombrar las columnas del dataframe a nombres canónicos esperados, basándose en el orden de las columnas
def canonicalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas según la posición observada en el profiling.
    """
    expected_columns = [
        "num_corre",
        "depto_boleta",
        "municipio_boleta",
        "mes",
        "anio",
        "tipo_falta",
        "sexo",
        "edad",
        "grupo_etnico",
        "estado_civil",
        "depto_nacionalidad",
        "condicion_alfabetismo",
        "escolaridad",
        "ocupacion",
        "area_geografica",
        "estado_ebriedad",
        "grupo_etario_1",
        "grupo_etario_2",
        "grupo_etario_texto",
        "depto_nacimiento",
        "nacionalidad",
        "subgrupo_ocupacion",
        "gran_grupo_ocupacion",
    ]

    if len(df.columns) < len(expected_columns):
        raise ValueError(
            f"Se esperaban al menos {len(expected_columns)} columnas, pero llegaron {len(df.columns)}"
        )

    rename_map = {}
    for idx, new_name in enumerate(expected_columns):
        rename_map[df.columns[idx]] = new_name

    df = df.rename(columns=rename_map)
    return df

#metodo para obtener o crear una fuente de dati
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

#metodo para obtener o crear una fecha, devolviendo su id
def get_or_create_fecha(repo: FirebirdRepository, anio: int, mes: int, dia: int = 1) -> int:
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

#metodo para limpiar y validar la edad, devolviendo None si no es válida o si es considerada "ignorada"
def clean_edad(value):
    edad = safe_int(value)
    if edad is None:
        return None

    if edad in {99, 999, 9999}:
        return None

    if edad < 0 or edad > 120:
        return None

    return edad

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

#metodo para obtener o crear un tipo de falta, devolviendo su id
def get_or_create_tipo_falta(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM tipo_falta
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="TF", max_len=20)

    repo.execute("""
        INSERT INTO tipo_falta (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))

    return repo.fetch_one()[0]

#metodo para obtener o crear un grupo etario, devolviendo su id
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

    codigo = build_unique_code(nombre, prefix="GE", max_len=20)

    repo.execute("""
        INSERT INTO grupo_etario (codigo, nombre, edad_min, edad_max, tipo_grupo)
        VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, None, None, "Falta judicial"))

    return repo.fetch_one()[0]

#metodo para obtener o crear grupo etnico, retornando el id
def get_or_create_grupo_etnico(repo: FirebirdRepository, nombre: str):
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

#metodo para obtener o crear condicion de alfabetismo, devolviendo su id
def get_or_create_condicion_alfabetismo(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM condicion_alfabetismo
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="ALF", max_len=20)

    repo.execute("""
        INSERT INTO condicion_alfabetismo (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))

    return repo.fetch_one()[0]

#metodo para obtener o crear escolaridad, devolviendo su id
def get_or_create_escolaridad(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM escolaridad
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO escolaridad (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))

    return repo.fetch_one()[0]

#metodo para obtener o crear areageografica, devolviendo su id
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

#metodo para obtener o crear estado ebriedad, devolviendo su id
def get_or_create_estado_ebriedad(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM estado_ebriedad
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="EBR", max_len=20)

    repo.execute("""
        INSERT INTO estado_ebriedad (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))

    return repo.fetch_one()[0]

#metodo para obtener o crear ocpuacion, devolviendo su id
def get_or_create_ocupacion(repo: FirebirdRepository, nombre: str):
    if not nombre:
        return None

    repo.execute("""
        SELECT id
        FROM ocupacion
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    codigo = build_unique_code(nombre, prefix="OC", max_len=20)

    repo.execute("""
        SELECT id
        FROM ocupacion
        WHERE codigo = ?
    """, (codigo,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO ocupacion (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))

    return repo.fetch_one()[0]

#metodo para crear una persona, devolviendo su id
def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]

#metodo para insertar una falta judicial, recibiendo todos los ids necesarios y el id de la fuente de dato
def insert_falta_judicial(
    repo: FirebirdRepository,
    id_fecha: int,
    id_municipio: int,
    id_tipo_falta: int,
    id_persona: int,
    id_grupo_etario: int | None,
    id_grupo_etnico: int | None,
    id_condicion_alfabetismo: int | None,
    id_escolaridad: int | None,
    id_area_geografica: int | None,
    id_estado_ebriedad: int | None,
    id_ocupacion: int | None,
    id_fuente_dato: int
):
    repo.execute("""
        INSERT INTO falta_judicial (
            id_fecha,
            id_municipio,
            id_tipo_falta,
            id_persona,
            id_grupo_etario,
            id_grupo_etnico,
            id_condicion_alfabetismo,
            id_escolaridad,
            id_area_geografica,
            id_estado_ebriedad,
            id_ocupacion,
            id_fuente_dato
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        id_fecha,
        id_municipio,
        id_tipo_falta,
        id_persona,
        id_grupo_etario,
        id_grupo_etnico,
        id_condicion_alfabetismo,
        id_escolaridad,
        id_area_geografica,
        id_estado_ebriedad,
        id_ocupacion,
        id_fuente_dato
    ))

# metodo para limpiar y normalizar valores de catalogos "ignorado"
def clean_catalog_value(value: str, default: str = "Ignorado") -> str:
    text = normalize_text(value)
    norm = normalize_name(text)

    if norm in {"", "sd", "s/d", "ignorado", "ignorada", "999", "9999", "99"}:
        return default

    return text

#metodo para convertir el nombre del mes en español a su numero correspondiente, devolviendo None si no se reconoce
def parse_mes_to_int(mes_texto: str):
    mes_norm = normalize_name(mes_texto)
    return MESES_ES.get(mes_norm)

#ejecutor etl
def run_faltas_judiciales_etl(
    repo: FirebirdRepository,
    file_path: str,
    dataset_name: str = "Faltas judiciales"
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print(f"Procesando archivo: {file_path}")
    # Cargar el archivo Excel y normalizar columnas
    df = pd.read_excel(file_path, sheet_name="Sheet1", header=0)
    df = canonicalize_dataframe_columns(df)

    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    municipio_name_map = build_municipio_name_map(repo)

    inserted = 0
    skipped_missing_municipio = 0
    skipped_missing_mes = 0
    skipped_missing_sexo = 0
    skipped_invalid_year = 0
    skipped_missing_tipo_falta = 0
    #recorriendo cada elemento en el df para insertar atenciones brindadas
    for _, row in df.iterrows():
        anio = safe_int(row.get("anio"))
        mes = parse_mes_to_int(row.get("mes"))

        if anio is None:
            skipped_invalid_year += 1
            continue
        if mes is None:
            skipped_missing_mes += 1
            continue
        try:
            fecha_id = get_or_create_fecha(repo, anio, mes, 1)
        except Exception:
            skipped_invalid_year += 1
            continue

        municipio_nombre = normalize_text(row.get("municipio_boleta"))
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

        tipo_falta_nombre = normalize_text(row.get("tipo_falta"))
        if not tipo_falta_nombre:
            skipped_missing_tipo_falta += 1
            continue
        id_tipo_falta = get_or_create_tipo_falta(repo, tipo_falta_nombre)

        sexo_nombre = normalize_text(row.get("sexo"))
        if normalize_name(sexo_nombre) in {"", "sd", "s/d", "999", "9"}:
            skipped_missing_sexo += 1
            sexo_nombre = "Ignorado"
        id_sexo = get_or_create_sexo(repo, sexo_nombre)

        edad = clean_edad(row.get("edad"))

        grupo_etnico_nombre = clean_catalog_value(row.get("grupo_etnico"))
        alfabetismo_nombre = clean_catalog_value(row.get("condicion_alfabetismo"))
        escolaridad_nombre = clean_catalog_value(row.get("escolaridad"))
        area_geografica_nombre = clean_catalog_value(row.get("area_geografica"))
        estado_ebriedad_nombre = clean_catalog_value(row.get("estado_ebriedad"))
        grupo_etario_nombre = clean_catalog_value(row.get("grupo_etario_texto"))
        ocupacion_nombre = clean_catalog_value(row.get("ocupacion"))

        id_grupo_etnico = get_or_create_grupo_etnico(repo, grupo_etnico_nombre) if grupo_etnico_nombre else None
        id_condicion_alfabetismo = get_or_create_condicion_alfabetismo(repo, alfabetismo_nombre) if alfabetismo_nombre else None
        id_escolaridad = get_or_create_escolaridad(repo, escolaridad_nombre) if escolaridad_nombre else None
        id_area_geografica = get_or_create_area_geografica(repo, area_geografica_nombre) if area_geografica_nombre else None
        id_estado_ebriedad = get_or_create_estado_ebriedad(repo, estado_ebriedad_nombre) if estado_ebriedad_nombre else None
        id_grupo_etario = get_or_create_grupo_etario(repo, grupo_etario_nombre) if grupo_etario_nombre else None
        id_ocupacion = get_or_create_ocupacion(repo, ocupacion_nombre) if ocupacion_nombre else None

        persona_id = create_persona(repo, id_sexo, edad)

        insert_falta_judicial(
            repo=repo,
            id_fecha=fecha_id,
            id_municipio=municipio_id,
            id_tipo_falta=id_tipo_falta,
            id_persona=persona_id,
            id_grupo_etario=id_grupo_etario,
            id_grupo_etnico=id_grupo_etnico,
            id_condicion_alfabetismo=id_condicion_alfabetismo,
            id_escolaridad=id_escolaridad,
            id_area_geografica=id_area_geografica,
            id_estado_ebriedad=id_estado_ebriedad,
            id_ocupacion=id_ocupacion,
            id_fuente_dato=fuente_id
        )

        inserted += 1
        if inserted % 1000 == 0:
            print(f"Procesados correctamente: {inserted}")

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos por municipio no encontrado: {skipped_missing_municipio}")
    print(f"Omitidos por mes no reconocido: {skipped_missing_mes}")
    print(f"Omitidos por sexo no reconocido: {skipped_missing_sexo}")
    print(f"Omitidos por año inválido: {skipped_invalid_year}")
    print(f"Omitidos por tipo de falta faltante: {skipped_missing_tipo_falta}")