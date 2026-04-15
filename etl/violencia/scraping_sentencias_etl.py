from datetime import date, datetime
import hashlib
import unicodedata
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from repositories.firebird_repository import FirebirdRepository


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL_SENTENCIAS = "https://observatorio.mp.gob.gt/sentencias/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://observatorio.mp.gob.gt/",
}

FISCALIAS_VALIDAS = {
    "Fiscalía de Sección de la Mujer",
    "Fiscalía contra el Delito de Femicidio",
    "Fiscalia de la Niñez y Adolescencia",
    "Fiscalía de la Niñez y Adolescencia",
}

MAP_LABELS = {
    "Ubicación": "ubicacion",
    "Sentenciado": "sentenciado",
    "Delito": "delito",
    "Fecha de sentencia": "fecha_sentencia",
    "Descripción del hecho": "descripcion_hecho",
    "Sentencia": "sentencia_texto",
    "Sentencia en primer grado": "sentencia_primer_grado",
    "Reparación digna": "reparacion_digna",
    "Reparacion digna": "reparacion_digna",
    "Medidas de reparación": "medidas_reparacion",
    "Medidas de reparacion": "medidas_reparacion",
    "Medidas victimológicas": "medidas_victimologicas",
    "Medidas victimologicas": "medidas_victimologicas",
    "Garantía de no repetición": "garantia_no_repeticion",
    "Garantia de no repeticion": "garantia_no_repeticion",
    "Garantías de no repetición": "garantias_no_repeticion",
    "Garantias de no repeticion": "garantias_no_repeticion",
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

def truncate_text(value: str, max_len: int):
    text = normalize_text(value)
    if not text:
        return text
    if len(text) <= max_len:
        return text
    return text[:max_len].strip()

def normalize_name(text) -> str:
    if text is None or pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.replace("(a)", "").replace("(o)", "")
    text = " ".join(text.split())
    return text


def clean_prefix_colon(value):
    text = normalize_text(value)
    if not text:
        return None

    if text.startswith(":"):
        text = text[1:].strip()

    return text if text else None


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


def normalize_fiscalia(value):
    text = normalize_text(value)

    replacements = {
        "Fiscalia de la Niñez y Adolescencia": "Fiscalía de la Niñez y Adolescencia",
        "Fiscalia de la Ninez y Adolescencia": "Fiscalía de la Niñez y Adolescencia",
    }

    return replacements.get(text, text)


def split_ubicacion(value):
    text = " ".join(normalize_text(value).split())

    if not text:
        return None, None

    if "," in text:
        partes = [x.strip() for x in text.split(",", 1)]
        municipio = partes[0] if len(partes) > 0 else None
        departamento = partes[1] if len(partes) > 1 else None
        return municipio, departamento

    return text, None


def parse_fecha_sentencia(fecha_texto: str):
    text = normalize_text(fecha_texto)
    if not text:
        return None

    try:
        return datetime.strptime(text, "%d/%m/%Y").date()
    except Exception:
        return None


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


def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    dataset_name = truncate_text(dataset_name, 200)

    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("MP Observatorio", dataset_name, "Web Scraping"))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("MP Observatorio", dataset_name, "Web Scraping"))
    return repo.fetch_one()[0]


def get_or_create_fecha(repo: FirebirdRepository, fecha_obj: date) -> int:
    fecha_str = fecha_obj.strftime("%Y-%m-%d")

    repo.execute("""
        SELECT id
        FROM fecha
        WHERE fecha = ?
    """, (fecha_str,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fecha (fecha, anio, mes, nombre_mes, dia, dia_semana)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (
        fecha_str,
        fecha_obj.year,
        fecha_obj.month,
        NOMBRES_MESES_ES[fecha_obj.month],
        fecha_obj.day,
        DIAS_ES[fecha_obj.weekday()]
    ))
    return repo.fetch_one()[0]


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


def build_municipio_dep_map(repo: FirebirdRepository) -> dict:
    get_or_create_municipio_ignorado(repo)

    repo.execute("""
        SELECT m.id, m.nombre, d.nombre
        FROM municipio m
        JOIN departamento d ON d.id = m.id_departamento
    """)
    rows = repo.fetch_all()

    result = {}
    for municipio_id, municipio_nombre, departamento_nombre in rows:
        key = (normalize_name(departamento_nombre), normalize_name(municipio_nombre))
        result[key] = municipio_id
    return result

def debug_len(label: str, value):
    text = normalize_text(value)
    print(f"{label} | len={len(text)} | value={text}")

def get_or_create_tipo_fallo(repo: FirebirdRepository, nombre: str) -> int:
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


def get_or_create_delito(repo: FirebirdRepository, nombre: str, categoria_nombre: str | None = None):
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


def get_or_create_despacho_judicial(repo: FirebirdRepository, nombre: str) -> int:
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


def get_or_create_hecho_delictivo(
    repo: FirebirdRepository,
    id_fecha: int,
    id_municipio: int,
    id_delito: int
) -> int:
    repo.execute("""
        SELECT id
        FROM hecho_delictivo
        WHERE id_fecha = ?
          AND id_municipio = ?
          AND id_delito = ?
          AND id_zona IS NULL
          AND id_area_geografica IS NULL
          AND id_franja_horaria IS NULL
    """, (
        id_fecha,
        id_municipio,
        id_delito
    ))
    row = repo.fetch_one()
    if row:
        return row[0]

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
        None
    ))
    return repo.fetch_one()[0]


def get_or_create_proceso_judicial(
    repo: FirebirdRepository,
    id_hecho_delictivo: int,
    id_departamento: int,
    id_delito: int
) -> int:
    repo.execute("""
        SELECT id
        FROM proceso_judicial
        WHERE id_hecho_delictivo = ?
          AND id_departamento = ?
          AND id_delito = ?
    """, (
        id_hecho_delictivo,
        id_departamento,
        id_delito
    ))
    row = repo.fetch_one()
    if row:
        return row[0]

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


def get_or_create_sentencia(
    repo: FirebirdRepository,
    id_proceso_judicial: int,
    id_tipo_fallo: int
) -> int:
    repo.execute("""
        SELECT id
        FROM sentencia
        WHERE id_proceso_judicial = ?
          AND id_tipo_fallo = ?
    """, (
        id_proceso_judicial,
        id_tipo_fallo
    ))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO sentencia (id_proceso_judicial, id_tipo_fallo)
        VALUES (?, ?)
        RETURNING id
    """, (
        id_proceso_judicial,
        id_tipo_fallo
    ))
    return repo.fetch_one()[0]


def fetch_html() -> str:
    response = requests.get(
        URL_SENTENCIAS,
        headers=HEADERS,
        timeout=30,
        verify=False
    )
    response.raise_for_status()
    return response.text


def extract_main_lines() -> list[str]:
    html = fetch_html()
    soup = BeautifulSoup(html, "html.parser")

    contenedor = None
    for row in soup.find_all("div", class_="row"):
        text = row.get_text(" ", strip=True).lower()
        if "sentenciado" in text and "delito" in text and "fecha de sentencia" in text:
            contenedor = row
            break

    if contenedor is None:
        raise ValueError("No se encontró contenedor principal de sentencias")

    texto = contenedor.get_text("\n", strip=True)
    lineas = [normalize_text(x) for x in texto.splitlines() if normalize_text(x)]

    return lineas


def split_sentencias(lineas: list[str]) -> list[list[str]]:
    indices = [i for i, x in enumerate(lineas) if x in FISCALIAS_VALIDAS]

    bloques = []
    for idx, start in enumerate(indices):
        end = indices[idx + 1] if idx + 1 < len(indices) else len(lineas)
        bloque = lineas[start:end]
        if bloque:
            bloques.append(bloque)

    return bloques


def parse_bloque_sentencia(bloque: list[str]) -> dict:
    data = {
        "fiscalia": None,
        "ubicacion": None,
        "sentenciado": None,
        "delito": None,
        "fecha_sentencia": None,
        "descripcion_hecho": None,
        "sentencia_texto": None,
        "sentencia_primer_grado": None,
        "reparacion_digna": None,
        "medidas_reparacion": None,
        "medidas_victimologicas": None,
        "garantia_no_repeticion": None,
        "garantias_no_repeticion": None,
    }

    if not bloque:
        return data

    data["fiscalia"] = bloque[0]

    i = 1
    while i < len(bloque):
        line = bloque[i]

        if line == ":":
            i += 1
            continue

        if line not in MAP_LABELS:
            i += 1
            continue

        field_name = MAP_LABELS[line]
        i += 1

        values = []
        while i < len(bloque):
            current = bloque[i]

            if current == ":":
                i += 1
                continue

            if current in MAP_LABELS:
                break

            values.append(current)
            i += 1

        value = " ".join(values).strip()
        if value:
            data[field_name] = value

    return data


def clean_sentencias_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in [
        "sentencia_primer_grado",
        "reparacion_digna",
        "medidas_reparacion",
        "medidas_victimologicas",
        "garantia_no_repeticion",
        "garantias_no_repeticion",
        "sentencia_texto",
    ]:
        if col in df.columns:
            df[col] = df[col].apply(clean_prefix_colon)

    df["fiscalia"] = df["fiscalia"].apply(normalize_fiscalia)

    ubicaciones = df["ubicacion"].apply(split_ubicacion)
    df["municipio_sentencia"] = ubicaciones.apply(lambda x: x[0])
    df["departamento_sentencia"] = ubicaciones.apply(lambda x: x[1])

    df["fecha_obj"] = df["fecha_sentencia"].apply(parse_fecha_sentencia)
    df["tipo_fallo"] = "Sentencia en primer grado"

    return df


def build_sentencias_dataframe() -> pd.DataFrame:
    print("Extrayendo HTML de sentencias...")
    lineas = extract_main_lines()
    bloques = split_sentencias(lineas)

    print(f"Total líneas: {len(lineas)}")
    print(f"Bloques de sentencias detectados: {len(bloques)}")

    records = []
    for i, bloque in enumerate(bloques, start=1):
        record = parse_bloque_sentencia(bloque)
        records.append(record)

        if i <= 2:
            print(f"Ejemplo sentencia {i}:")
            print(record)

    df = pd.DataFrame(records)
    df = clean_sentencias_dataframe(df)

    df = df.drop_duplicates(subset=[
        "fiscalia",
        "ubicacion",
        "delito",
        "fecha_sentencia",
        "sentencia_primer_grado"
    ])

    return df


def run_sentencias_detalladas_etl(
    repo: FirebirdRepository,
    dataset_name: str = "Sentencias detalladas observatorio MP"
):
    print("Iniciando ETL de sentencias detalladas")

    df = build_sentencias_dataframe()

    if df.empty:
        raise ValueError("No se encontraron sentencias para procesar")

    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    departamento_map = build_departamento_name_map(repo)
    municipio_dep_map = build_municipio_dep_map(repo)

    departamento_ignorado_id = get_or_create_departamento_ignorado(repo)
    municipio_ignorado_id = get_or_create_municipio_ignorado(repo)

    insertados = 0
    sin_fecha = 0
    sin_departamento = 0
    sin_municipio = 0
    sin_delito = 0
    duplicados = 0

    for _, row in df.iterrows():
        fiscalia = truncate_text(normalize_text(row.get("fiscalia")), 150)
        municipio_nombre = truncate_text(normalize_text(row.get("municipio_sentencia")), 100)
        departamento_nombre = truncate_text(normalize_text(row.get("departamento_sentencia")), 100)
        delito_nombre = truncate_text(normalize_text(row.get("delito")), 150)
        fecha_obj = row.get("fecha_obj")
        tipo_fallo_nombre = truncate_text(normalize_text(row.get("tipo_fallo")), 100)

        if not fecha_obj:
            sin_fecha += 1
            continue

        if not delito_nombre:
            sin_delito += 1
            continue

        try:

            fecha_id = get_or_create_fecha(repo, fecha_obj)

            dep_norm = normalize_name(departamento_nombre)
            departamento_id = departamento_map.get(dep_norm)
            if not departamento_id:
                sin_departamento += 1
                departamento_id = departamento_ignorado_id

            mun_norm = normalize_name(municipio_nombre)
            municipio_id = municipio_dep_map.get((dep_norm, mun_norm))
            if not municipio_id:
                sin_municipio += 1
                municipio_id = municipio_ignorado_id

            delito_id = get_or_create_delito(repo, delito_nombre, None)
            tipo_fallo_id = get_or_create_tipo_fallo(repo, tipo_fallo_nombre)

            if fiscalia:
                get_or_create_despacho_judicial(repo, truncate_text(fiscalia, 150))

            hecho_id = get_or_create_hecho_delictivo(
                repo=repo,
                id_fecha=fecha_id,
                id_municipio=municipio_id,
                id_delito=delito_id
            )

            proceso_id = get_or_create_proceso_judicial(
                repo=repo,
                id_hecho_delictivo=hecho_id,
                id_departamento=departamento_id,
                id_delito=delito_id
            )
        except Exception as e:
            print("ERROR DETECTADO EN REGISTRO:")
            print(row.to_dict())
            print("DETALLE:", e)
            raise

        repo.execute("""
            SELECT id
            FROM sentencia
            WHERE id_proceso_judicial = ?
              AND id_tipo_fallo = ?
        """, (proceso_id, tipo_fallo_id))
        existing = repo.fetch_one()
        if existing:
            duplicados += 1
            continue

        get_or_create_sentencia(
            repo=repo,
            id_proceso_judicial=proceso_id,
            id_tipo_fallo=tipo_fallo_id
        )

        insertados += 1

        if insertados % 100 == 0:
            print(f"Insertados correctamente: {insertados}")

    repo.commit()

    print(f"Insertados: {insertados}")
    print(f"Omitidos por fecha inválida: {sin_fecha}")
    print(f"Departamentos no encontrados: {sin_departamento}")
    print(f"Municipios no encontrados: {sin_municipio}")
    print(f"Omitidos por delito faltante: {sin_delito}")
    print(f"Omitidos por duplicado: {duplicados}")
    print(f"Fuente registrada con id: {fuente_id}")