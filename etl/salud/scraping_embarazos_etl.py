import re
import unicodedata

import pandas as pd
import requests
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from repositories.firebird_repository import FirebirdRepository


BASE_URL = "https://osarguatemala.org/embarazo/datos/"
# Headers para la solicitud HTTP al sitio
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}
# diccionarios para edades,
EDADES = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

#metodo para normalizar texto eliminando espacios
def normalize_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()

# metodo para truncar texto a una longitud máxima, eliminando espacios
def normalize_name(text) -> str:
    if text is None or pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.replace("(a)", "").replace("(o)", "")
    text = " ".join(text.split())
    return text

#metodo para convertir a entero de forma segura, devolviendo None si no se puede convertir o si el valor es considerado "nan"
def safe_int(value):
    if value is None or pd.isna(value):
        return None
    try:
        text = str(value).strip()
        if normalize_name(text) in {"", "nan", "sd", "s/d"}:
            return None
        return int(float(text))
    except Exception:
        return None

#metodo para construir una sesión de requests con reintentos configurados
def build_session() -> requests.Session:
    session = requests.Session()

    retries = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://osarguatemala.org/",
        "Connection": "keep-alive",
    })
    return session

#metodo para realizar una solicitud GET a una URL y devolver el contenido, con manejo de errores y reintentos
def fetch_url(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=30, allow_redirects=True)

    print(f"GET {url} -> {response.status_code}")
    print(f"URL final: {response.url}")

    response.raise_for_status()
    return response.text

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

#metodo para obtener o crear una fuente de dati
def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("OSAR Guatemala", dataset_name, "Web Scraping"))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("OSAR Guatemala", dataset_name, "Web Scraping"))
    return repo.fetch_one()[0]

#metodo para obtener o crear un tipo_indicador_salud
def get_or_create_tipo_indicador_salud(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM tipo_indicador_salud
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO tipo_indicador_salud (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]

#metodo para obtener o crear sexo, retornando el id
def get_or_create_sexo(repo: FirebirdRepository, nombre: str) -> int:
    nombre_norm = normalize_name(nombre)

    if nombre_norm in {"mujer", "mujeres", "femenino"}:
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

#metodo para obtener o crear una fecha, devolviendo su id
def get_or_create_fecha(repo: FirebirdRepository, anio: int, mes: int, dia: int) -> int:
    fecha_str = f"{anio:04d}-{mes:02d}-{dia:02d}"

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
    """, (fecha_str, anio, mes, "Enero", dia, "Desconocido"))
    return repo.fetch_one()[0]

#metodo para obtener o crear grupo etario, retornando el id
def get_or_create_grupo_etario(repo: FirebirdRepository, edad: int) -> int:
    nombre = f"{edad} años"
    codigo = f"E{edad}"

    repo.execute("""
        SELECT id
        FROM grupo_etario
        WHERE codigo = ?
    """, (codigo,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO grupo_etario (codigo, nombre, edad_min, edad_max, tipo_grupo)
        VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, edad, edad, "Edad exacta"))
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

#metodo para obtener post links de la categoría de embarazos en el sitio de OSAR, filtrando por títulos que contengan palabras clave relacionadas con registros de nacimientos y año
def get_post_links(max_pages: int = 5) -> list[str]:
    session = build_session()
    links = []
    seen = set()

    for page in range(1, max_pages + 1):
        url = BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"
        print(f"Revisando categoría: {url}")
        
        try:
            html = fetch_url(session, url)
        except Exception as e:
            print(f"No se pudo leer la página: {url}")
            print(f"Detalle: {e}")

            if "404" in str(e):
                break

            continue

        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select("h2.entry-title a, h2 a"):
            href = a.get("href")
            title = normalize_text(a.get_text(" ", strip=True))
            title_norm = normalize_name(title)

            if not href:
                continue

            if "registros de nacimientos" not in title_norm and "registros de nacimiento" not in title_norm:
                continue

            if "ano" not in title_norm:
                continue

            if href not in seen:
                seen.add(href)
                links.append(href)
                print(f"Post encontrado: {title}")

    return links

#metodo para extraer el año de un title usando expresiones regulares
def extract_year_from_title(title: str):
    match = re.search(r"\b(20\d{2})\b", title)
    if match:
        return int(match.group(1))
    return None

#metodo apra extraer el texto de un post dado su URL, devolviendo el título y el contenido normalizados, e imprimiendo el título detectado y los primeros 1000 caracteres del contenido para verificación
def extract_post_text(url: str) -> tuple[str, str]:
    session = build_session()
    html = fetch_url(session, url)

    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.select_one("h1.entry-title")
    title = normalize_text(title_tag.get_text(" ", strip=True)) if title_tag else ""

    article = (
        soup.select_one("article")
        or soup.select_one("main")
        or soup.select_one("body")
    )

    if not article:
        return title, ""

    content_text = article.get_text("\n", strip=True)
    content_text = normalize_text(content_text)

    print(f"Título detectado: {title}")
    print("Primeros 1000 caracteres del contenido:")
    print(content_text[:1000])

    return title, content_text

#metodo para validar si el post contiene una tabla con formato esperado, buscando palabras clave en el título y el contenido
def is_tabular_post(title: str, content_text: str) -> bool:
    text_norm = normalize_name(content_text)
    title_norm = normalize_name(title)

    if "ano" not in title_norm:
        return False

    if "departamento/municipio" not in text_norm:
        return False

    if "10 11 12 13 14 15 16 17 18 19 total" not in text_norm:
        return False

    return True

#metodo para extraer los tokens de la tabla del contenido del post, buscando el bloque de texto que contiene el encabezado esperado
def extract_table_tokens(content_text: str) -> list[str]:
    lines = [normalize_text(x) for x in content_text.splitlines()]
    lines = [x for x in lines if x]

    start_idx = None

    esperado = [
        "departamento/municipio",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "total",
    ]

    for i in range(len(lines) - 11):
        bloque = [normalize_name(x) for x in lines[i:i + 12]]
        if bloque == esperado:
            start_idx = i + 12
            break

    if start_idx is None:
        print("No se encontró encabezado exacto por líneas.")
        raise ValueError("No se encontró encabezado de tabla")

    tokens = lines[start_idx:]
    return tokens

#metodo para parsear los tokens de la tabla, construyendo una lista de diccionarios con los campos anio, departamento, municipio, edad y cantidad
def parse_table_tokens(tokens: list[str], anio: int) -> list[dict]:
    departamentos_validos = get_departamentos_validos()

    records = []
    i = 0
    departamento_actual = None

    while i < len(tokens):
        token_norm = normalize_name(tokens[i])

        if token_norm == "total general":
            break

        if token_norm in departamentos_validos:
            departamento_actual = tokens[i]
            i += 1
            continue

        if token_norm == "total":
            i += 1
            count_nums = 0
            while i < len(tokens) and count_nums < 11 and re.fullmatch(r"\d+", normalize_text(tokens[i])):
                i += 1
                count_nums += 1
            continue

        if not departamento_actual:
            i += 1
            continue

        nombre_tokens = []
        while i < len(tokens):
            t = normalize_text(tokens[i])
            if re.fullmatch(r"\d+", t):
                break
            nombre_tokens.append(t)
            i += 1

        if not nombre_tokens:
            i += 1
            continue

        municipio = " ".join(nombre_tokens).strip()

        valores = []
        while i < len(tokens) and len(valores) < 11:
            t = normalize_text(tokens[i])
            if re.fullmatch(r"\d+", t):
                valores.append(int(t))
                i += 1
            else:
                break

        if len(valores) == 10:
            valores.append(sum(valores))

        if len(valores) != 11:            
            continue
        
        for idx, edad in enumerate(EDADES):
            records.append({
                "anio": anio,
                "departamento": normalize_text(departamento_actual),
                "municipio": normalize_text(municipio),
                "edad": edad,
                "cantidad": valores[idx],
            })

    return records

#metodo para parsear las filas de la tabla, intentando separar el nombre del municipio de los valores numéricos, y devolviendo el nombre normalizado y la lista de valores enteros
def parse_table_row(line: str):
    tokens = line.split()

    if len(tokens) < 11:
        return None, None

    for numeric_count in (11, 10):
        if len(tokens) < numeric_count + 1:
            continue

        tail = tokens[-numeric_count:]
        head = tokens[:-numeric_count]

        ok = True
        values = []
        for x in tail:
            if not re.fullmatch(r"\d+", x):
                ok = False
                break
            values.append(int(x))

        if ok:
            name = " ".join(head).strip()
            if not name:
                return None, None

            if numeric_count == 10:
                total = sum(values)
                values.append(total)

            return name, values

    return None, None

#metodo para obtener el conjunto de nombres de departamentos válidos y normalizados
def get_departamentos_validos() -> set:
    return {
        "alta verapaz",
        "baja verapaz",
        "chimaltenango",
        "chiquimula",
        "el progreso",
        "escuintla",
        "guatemala",
        "huehuetenango",
        "izabal",
        "jalapa",
        "jutiapa",
        "peten",
        "quetzaltenango",
        "quiche",
        "retalhuleu",
        "sacatepequez",
        "san marcos",
        "santa rosa",
        "solola",
        "suchitepequez",
        "totonicapan",
        "zacapa",
    }

#metodo para parsear posts y extraer los registros de embarazos
def parse_post_to_records(url: str) -> list[dict]:
    title, content_text = extract_post_text(url)

    anio = extract_year_from_title(title)
    if anio is None:
        print(f"No se pudo extraer año de: {title}")
        return []

    try:
        tokens = extract_table_tokens(content_text)
    except Exception as e:
        print(f"No se pudo extraer tabla de {title}: {e}")
        return []

    records = parse_table_tokens(tokens, anio)

    print(f"Registros extraídos de {title}: {len(records)}")
    return records

#metodo para construir el dataframe de embarazos, ejecutando el proceso completo de extracción, parseo y limpieza
def build_embarazos_dataframe(max_pages: int = 5) -> pd.DataFrame:
    links = get_post_links(max_pages=max_pages)

    all_records = []
    for link in links:
        print(f"Procesando post: {link}")
        try:
            all_records.extend(parse_post_to_records(link))
        except Exception as e:
            print(f"Error procesando {link}: {e}")

    df = pd.DataFrame(all_records)

    if df.empty:
        return df

    df["anio"] = df["anio"].apply(safe_int)
    df["edad"] = df["edad"].apply(safe_int)
    df["cantidad"] = df["cantidad"].apply(safe_int)

    df["departamento"] = df["departamento"].apply(normalize_text)
    df["municipio"] = df["municipio"].apply(normalize_text)

    df = df.drop_duplicates(subset=["anio", "departamento", "municipio", "edad"])

    return df

#metodo para insertar un registro de salud de embarazos adolescentes, recibiendo los ids necesarios y la cantidad, e insertando en la tabla registro_salud
def insert_registro_salud(
    repo: FirebirdRepository,
    id_tipo_indicador_salud: int,
    id_municipio: int,
    id_fecha: int,
    id_grupo_etario: int,
    id_sexo: int,
    cantidad: int,
    id_fuente_dato: int
):
    repo.execute("""
        INSERT INTO registro_salud (
            id_tipo_indicador_salud,
            id_enfermedad,
            id_diagnostico,
            id_municipio,
            id_fecha,
            id_grupo_etario,
            id_sexo,
            cantidad,
            id_fuente_dato
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        id_tipo_indicador_salud,
        None,
        None,
        id_municipio,
        id_fecha,
        id_grupo_etario,
        id_sexo,
        cantidad,
        id_fuente_dato
    ))

#metodo para validar si ya existe un registro de salud con los mismos parámetros, para evitar duplicados
def exists_registro_salud(
    repo: FirebirdRepository,
    id_tipo_indicador_salud: int,
    id_municipio: int,
    id_fecha: int,
    id_grupo_etario: int,
    id_sexo: int,
    id_fuente_dato: int
) -> bool:
    repo.execute("""
        SELECT id
        FROM registro_salud
        WHERE id_tipo_indicador_salud = ?
          AND id_municipio = ?
          AND id_fecha = ?
          AND id_grupo_etario = ?
          AND id_sexo = ?
          AND id_fuente_dato = ?
    """, (
        id_tipo_indicador_salud,
        id_municipio,
        id_fecha,
        id_grupo_etario,
        id_sexo,
        id_fuente_dato
    ))
    row = repo.fetch_one()
    return row is not None

#ejecutor etl
def run_embarazos_etl(
    repo: FirebirdRepository,
    dataset_name: str = "Embarazos adolescentes OSAR"
):
    print("Iniciando ETL de embarazos OSAR")
    # Construir el dataframe de sentencias a partir del proceso de extracción y limpieza (5 paginas max)
    df = build_embarazos_dataframe(max_pages=5)

    if df.empty:
        raise ValueError("No se encontraron datos tabulares de embarazos")
    #obteniendo llaves foraneas necesarias para las inserciones
    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    tipo_indicador_id = get_or_create_tipo_indicador_salud(repo, "Embarazos adolescentes")
    sexo_id = get_or_create_sexo(repo, "Mujer")
    municipio_dep_map = build_municipio_dep_map(repo)
    municipio_ignorado_id = get_or_create_municipio_ignorado(repo)

    inserted = 0
    skipped_municipio = 0
    skipped_duplicado = 0
    #recorriendo cada elemento en el df para insertar los registros
    for _, row in df.iterrows():
        anio = safe_int(row.get("anio"))
        edad = safe_int(row.get("edad"))
        cantidad = safe_int(row.get("cantidad"))

        if anio is None or edad is None or cantidad is None:
            continue

        fecha_id = get_or_create_fecha(repo, anio, 1, 1)
        grupo_etario_id = get_or_create_grupo_etario(repo, edad)

        dep_norm = normalize_name(row.get("departamento"))
        mun_norm = normalize_name(row.get("municipio"))

        municipio_id = municipio_dep_map.get((dep_norm, mun_norm))
        if not municipio_id:
            skipped_municipio += 1
            municipio_id = municipio_ignorado_id

        if exists_registro_salud(
            repo,
            tipo_indicador_id,
            municipio_id,
            fecha_id,
            grupo_etario_id,
            sexo_id,
            fuente_id
        ):
            skipped_duplicado += 1
            continue

        insert_registro_salud(
            repo=repo,
            id_tipo_indicador_salud=tipo_indicador_id,
            id_municipio=municipio_id,
            id_fecha=fecha_id,
            id_grupo_etario=grupo_etario_id,
            id_sexo=sexo_id,
            cantidad=cantidad,
            id_fuente_dato=fuente_id
        )

        inserted += 1

        if inserted % 500 == 0:
            print(f"Insertados correctamente: {inserted}")

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Municipios no encontrados: {skipped_municipio}")
    print(f"Omitidos por duplicado: {skipped_duplicado}")