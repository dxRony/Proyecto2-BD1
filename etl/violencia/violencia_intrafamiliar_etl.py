from datetime import date
import hashlib
from pathlib import Path
import unicodedata
import pandas as pd

from repositories.firebird_repository import FirebirdRepository


MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

DIAS_ES = {
    0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
    4: "Viernes", 5: "Sábado", 6: "Domingo",
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
    text = " ".join(text.split())
    return text


def safe_int(value):
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except Exception:
        return None

def get_departamento_id_from_municipio(repo: FirebirdRepository, id_municipio: int) -> int | None:
    repo.execute("""
        SELECT id_departamento
        FROM municipio
        WHERE id = ?
    """, (id_municipio,))
    row = repo.fetch_one()
    return row[0] if row else None

def insert_proceso_judicial(
    repo: FirebirdRepository,
    id_hecho_delictivo: int,
    id_departamento: int,
    id_delito: int
) -> int:
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

def insert_sentencia(
    repo: FirebirdRepository,
    id_proceso_judicial: int,
    id_tipo_fallo: int
) -> int:
    repo.execute("""
        INSERT INTO sentencia (
            id_proceso_judicial,
            id_tipo_fallo
        )
        VALUES (?, ?)
        RETURNING id
    """, (
        id_proceso_judicial,
        id_tipo_fallo
    ))
    return repo.fetch_one()[0]

def get_or_create_tipo_fallo(repo: FirebirdRepository, codigo: str, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM tipo_fallo
        WHERE UPPER(codigo) = UPPER(?)
    """, (codigo,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO tipo_fallo (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]

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

def load_dictionary_maps(dict_path: str) -> dict:
    df = pd.read_excel(dict_path, sheet_name="DICCIONARIO.2023VIF", header=3)

    maps = {}
    current_code = None

    for _, row in df.iterrows():
        codigo = normalize_text(row.get("Código"))
        valor = row.get("Valor")
        etiqueta = normalize_text(row.get("Etiqueta"))

        if codigo:
            current_code = codigo
            if current_code not in maps:
                maps[current_code] = {}

        if current_code and valor is not None and not pd.isna(valor) and etiqueta:
            maps[current_code][safe_int(valor)] = etiqueta

    return maps


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
        MESES_ES[mes],
        dia,
        DIAS_ES[fecha_obj.weekday()]
    ))

    return repo.fetch_one()[0]


def get_or_create_fuente_dato(repo: FirebirdRepository, dataset_name: str) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("INE", dataset_name, "Excel"))

    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("INE", dataset_name, "Excel"))

    return repo.fetch_one()[0]


def build_departamento_map(repo: FirebirdRepository) -> dict:
    repo.execute("""
        SELECT id, codigo, nombre
        FROM departamento
    """)
    rows = repo.fetch_all()

    dep_map = {}

    for dep_id, codigo, _ in rows:
        if codigo is None:
            continue

        codigo_str = str(codigo).strip().upper()

        if codigo_str.startswith("D"):
            try:
                dep_map[int(codigo_str[1:])] = dep_id
                continue
            except Exception:
                pass

        try:
            dep_map[int(codigo_str)] = dep_id
        except Exception:
            pass

    return dep_map


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


def resolve_municipio_id_from_dict(row, maps, municipio_name_map):
    codigo = safe_int(row.get("HEC_DEPTOMCPIO"))
    if codigo is None:
        return None

    etiqueta = None
    if "HEC_DEPTOMCPIO" in maps:
        etiqueta = maps["HEC_DEPTOMCPIO"].get(codigo)
    if not etiqueta and "DEPTO_MCPIO" in maps:
        etiqueta = maps["DEPTO_MCPIO"].get(codigo)

    if not etiqueta:
        return None

    if "," in etiqueta:
        municipio_nombre = etiqueta.split(",")[-1].strip()
    else:
        municipio_nombre = etiqueta.strip()

    return municipio_name_map.get(normalize_name(municipio_nombre))


def get_or_create_sexo(repo: FirebirdRepository, nombre: str) -> int:
    nombre = normalize_text(nombre).lower()

    if nombre in {"hombre", "masculino"}:
        codigo = "H"
        nombre_final = "Hombre"
    elif nombre in {"mujer", "femenino"}:
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


def get_or_create_tipo_agresion(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM tipo_agresion
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO tipo_agresion (nombre)
        VALUES (?)
        RETURNING id
    """, (nombre,))
    return repo.fetch_one()[0]


def get_or_create_delito(repo: FirebirdRepository, codigo: str, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM delito
        WHERE LOWER(codigo) = LOWER(?)
    """, (codigo,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO delito (codigo, nombre, id_capitulo, id_categoria_delito)
        VALUES (?, ?, ?, ?)
        RETURNING id
    """, (codigo, nombre, None, None))

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
    id_municipio: int | None,
    id_delito: int
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
        None
    ))
    return repo.fetch_one()[0]


def get_agresor_data(row, maps):
    sexo_code = safe_int(row.get("AGRE_SEXO"))
    edad = safe_int(row.get("AGRE_EDAD"))

    sexo_nombre = None
    if sexo_code is not None:
        sexo_nombre = maps.get("AGRE_SEXO", {}).get(sexo_code)

    return sexo_nombre, edad


def insert_violencia_intrafamiliar(
    repo: FirebirdRepository,
    id_persona_victima: int,
    id_persona_agresor: int,
    id_hecho_delictivo: int,
    id_fuente_dato: int
):
    repo.execute("""
        INSERT INTO violencia_intrafamiliar (
            id_persona_victima,
            id_persona_agresor,
            id_hecho_delictivo,
            id_relacion_agresor,
            reiteracion_denuncia,
            id_fuente_dato_denuncia_previa,
            id_fuente_dato
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        id_persona_victima,
        id_persona_agresor,
        id_hecho_delictivo,
        None,
        None,
        None,
        id_fuente_dato
    ))


def is_valid_row(row) -> bool:
    anio = safe_int(row.get("HEC_ANO"))
    mes = safe_int(row.get("HEC_MES"))
    dia = safe_int(row.get("HEC_DIA"))

    if anio in {None, 9999}:
        return False
    if mes in {None, 99}:
        return False
    if dia in {None, 99}:
        return False

    return True


def run_violencia_intrafamiliar_etl(
    repo: FirebirdRepository,
    file_path: str,
    dict_path: str,
    dataset_name: str
):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    if not Path(dict_path).exists():
        raise FileNotFoundError(f"No existe el diccionario: {dict_path}")

    print(f"Procesando archivo: {file_path}")
    print(f"Usando diccionario: {dict_path}")

    maps = load_dictionary_maps(dict_path)
    df = pd.read_excel(file_path, sheet_name=0, header=0)

    fuente_id = get_or_create_fuente_dato(repo, dataset_name)
    dep_map = build_departamento_map(repo)
    municipio_name_map = build_municipio_name_map(repo)

    inserted = 0
    skipped_invalid_date = 0
    skipped_missing_fecha = 0
    skipped_missing_sexo = 0
    skipped_missing_departamento = 0
    skipped_missing_municipio = 0
    skipped_missing_tipo_agresion = 0

    for _, row in df.iterrows():
        if not is_valid_row(row):
            skipped_invalid_date += 1
            continue

        dia = safe_int(row.get("HEC_DIA"))
        mes = safe_int(row.get("HEC_MES"))
        anio = safe_int(row.get("HEC_ANO"))

        try:
            fecha_id = get_or_create_fecha(repo, anio, mes, dia)
        except Exception:
            skipped_missing_fecha += 1
            continue

        depto_code = safe_int(row.get("HEC_DEPTO"))
        if depto_code is None or depto_code not in dep_map:
            skipped_missing_departamento += 1
            continue

        municipio_id = resolve_municipio_id_from_dict(row, maps, municipio_name_map)
        if not municipio_id:
            skipped_missing_municipio += 1
            continue

        sexo_code = safe_int(row.get("VIC_SEXO"))
        sexo_nombre = maps.get("VIC_SEXO", {}).get(sexo_code)
        if not sexo_nombre:
            skipped_missing_sexo += 1
            continue

        sexo_id = get_or_create_sexo(repo, sexo_nombre)
        edad = safe_int(row.get("VIC_EDAD"))

        tipo_agresion_code = safe_int(row.get("HEC_TIPAGRE"))
        tipo_agresion_nombre = maps.get("HEC_TIPAGRE", {}).get(tipo_agresion_code)
        if not tipo_agresion_nombre:
            skipped_missing_tipo_agresion += 1
            continue

        get_or_create_tipo_agresion(repo, tipo_agresion_nombre)

        delito_codigo = f"VIF_{tipo_agresion_code}"
        delito_id = get_or_create_delito(repo, delito_codigo, tipo_agresion_nombre)

        persona_victima_id = create_persona(repo, sexo_id, edad)

        agresor_sexo_nombre, agresor_edad = get_agresor_data(row, maps)
        if agresor_sexo_nombre:
            agresor_sexo_id = get_or_create_sexo(repo, agresor_sexo_nombre)
        else:
            agresor_sexo_id = sexo_id

        persona_agresor_id = create_persona(repo, agresor_sexo_id, agresor_edad)

        hecho_id = create_hecho_delictivo(
            repo=repo,
            id_fecha=fecha_id,
            id_municipio=municipio_id,
            id_delito=delito_id
        )

        insert_violencia_intrafamiliar(
            repo=repo,
            id_persona_victima=persona_victima_id,
            id_persona_agresor=persona_agresor_id,
            id_hecho_delictivo=hecho_id,
            id_fuente_dato=fuente_id
        )
        
        
        departamento_id = get_departamento_id_from_municipio(repo, municipio_id)

        if departamento_id:
            # ejemplo académico: no todos los casos llegan a proceso
            if inserted % 3 == 0:
                proceso_id = insert_proceso_judicial(
                    repo=repo,
                    id_hecho_delictivo=hecho_id,
                    id_departamento=departamento_id,
                    id_delito=delito_id
                )

                # alternar tipos de fallo para que haya variedad
                if inserted % 5 == 0:
                    tipo_fallo_id = get_or_create_tipo_fallo(repo, "ABS", "ABSOLUTORIA")
                else:
                    tipo_fallo_id = get_or_create_tipo_fallo(repo, "COND", "CONDENATORIA")

                insert_sentencia(
                    repo=repo,
                    id_proceso_judicial=proceso_id,
                    id_tipo_fallo=tipo_fallo_id
                )

        inserted += 1

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos por fecha inválida en origen: {skipped_invalid_date}")
    print(f"Omitidos por fecha no encontrada en dimensión: {skipped_missing_fecha}")
    print(f"Omitidos por sexo no reconocido: {skipped_missing_sexo}")
    print(f"Omitidos por departamento no encontrado: {skipped_missing_departamento}")
    print(f"Omitidos por municipio no encontrado: {skipped_missing_municipio}")
    print(f"Omitidos por tipo de agresión no encontrado en diccionario: {skipped_missing_tipo_agresion}")