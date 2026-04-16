from pathlib import Path
import pandas as pd

from repositories.firebird_repository import FirebirdRepository
from utils.csv_utils import read_csv_file
from utils.normalizers import normalize_text, safe_int

#path del archivo a procesar
FILE_PATH = "Salud/Desnutricion/morbilidad-desnutricion-aguda-departamento-municipio-2012-a-2024.csv"

#metodos para obtener o crear registros relacionados (departamento, municipio, grupo etario, sexo, fecha)
def get_or_create_fuente_dato(repo: FirebirdRepository) -> int:
    repo.execute("""
        SELECT id
        FROM fuente_dato
        WHERE LOWER(institucion) = LOWER(?)
          AND LOWER(dataset) = LOWER(?)
          AND LOWER(tipo_fuente) = LOWER(?)
    """, ("MSPAS", "Desnutrición aguda", "CSV"))
    #si se encuentra, se retorna el id
    row = repo.fetch_one()
    if row:
        return row[0]
    #si no se encuentra, se inserta un nuevo registro y se retorna el nuevo id
    repo.execute("""
        INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
        VALUES (?, ?, ?)
        RETURNING id
    """, ("MSPAS", "Desnutrición aguda", "CSV"))
    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para obtener o crear tipo de indicador de salud
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
    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para obtener o crear enfermedad
def get_or_create_enfermedad(repo: FirebirdRepository, nombre: str, tipo: str) -> int:
    repo.execute("""
        SELECT id
        FROM enfermedad
        WHERE LOWER(nombre) = LOWER(?)
          AND LOWER(tipo) = LOWER(?)
    """, (nombre, tipo))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        INSERT INTO enfermedad (nombre, tipo)
        VALUES (?, ?)
        RETURNING id
    """, (nombre, tipo))
    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para obtener o crear departamento
def get_or_create_departamento(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM departamento
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("SELECT COUNT(*) FROM departamento")
    count_row = repo.fetch_one()
    correlativo = (count_row[0] or 0) + 1

    codigo_tmp = f"D{correlativo:03d}"

    repo.execute("""
        INSERT INTO departamento (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo_tmp, nombre))

    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para obtener o crear municipio
def get_or_create_municipio(repo: FirebirdRepository, nombre: str, id_departamento: int) -> int:
    repo.execute("""
        SELECT id
        FROM municipio
        WHERE LOWER(nombre) = LOWER(?)
          AND id_departamento = ?
    """, (nombre, id_departamento))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        SELECT COUNT(*)
        FROM municipio
        WHERE id_departamento = ?
    """, (id_departamento,))
    count_row = repo.fetch_one()
    correlativo = (count_row[0] or 0) + 1

    codigo_tmp = f"T{id_departamento:02d}{correlativo:03d}"

    repo.execute("""
        INSERT INTO municipio (codigo, nombre, id_departamento)
        VALUES (?, ?, ?)
        RETURNING id
    """, (codigo_tmp, nombre, id_departamento))

    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para obtener o crear grupo etario
def get_or_create_grupo_etario(repo: FirebirdRepository, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM grupo_etario
        WHERE LOWER(nombre) = LOWER(?)
    """, (nombre,))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("SELECT COUNT(*) FROM grupo_etario")
    count_row = repo.fetch_one()
    correlativo = (count_row[0] or 0) + 1

    codigo_tmp = f"G{correlativo:03d}"

    repo.execute("""
        INSERT INTO grupo_etario (codigo, nombre, edad_min, edad_max, tipo_grupo)
        VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, (codigo_tmp, nombre, None, None, "salud"))

    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para obtener o crear sexo
def get_or_create_sexo(repo: FirebirdRepository, codigo: str, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM sexo
        WHERE LOWER(codigo) = LOWER(?)
           OR LOWER(nombre) = LOWER(?)
    """, (codigo, nombre))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        INSERT INTO sexo (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para obtener o crear fecha
def get_or_create_fecha(repo: FirebirdRepository, anio: int) -> int:
    repo.execute("""
        SELECT id
        FROM fecha
        WHERE anio = ? AND mes = 1 AND dia = 1
    """, (anio,))
    row = repo.fetch_one()

    if row:
        return row[0]

    repo.execute("""
        INSERT INTO fecha (fecha, anio, mes, nombre_mes, dia, dia_semana)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (f"{anio}-01-01", anio, 1, "Enero", 1, "Lunes"))

    new_id = repo.fetch_one()[0]
    repo.commit()
    return new_id

#metodo para normalizar sexo
def normalize_sexo(value: str) -> tuple[str, str]:
    val = normalize_text(value)
    if val == "m":
        return "M", "Masculino"
    if val == "f":
        return "F", "Femenino"
    return "ND", "No definido"

#metodo para construir dataframe limpio a partir del csv
def build_dataframe(file_path: str) -> pd.DataFrame:
    df = read_csv_file(
    file_path=file_path,
    sep=";",
    encoding="utf-8-sig",
    normalize_headers=False
    )

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace("ñ", "n")
    )

    df = df[[
        "anio",
        "departamento",
        "municipio",
        "grupo_etario",
        "sexo",
        "cantidad"
    ]].copy()

    df["anio"] = df["anio"].apply(safe_int)
    df["cantidad"] = df["cantidad"].apply(safe_int)

    df["departamento"] = df["departamento"].astype(str).str.strip().str.title()
    df["municipio"] = df["municipio"].astype(str).str.strip().str.title()
    df["grupo_etario"] = df["grupo_etario"].astype(str).str.strip()
    df["sexo"] = df["sexo"].astype(str).str.strip().str.upper()

    df = df.dropna(subset=["anio", "cantidad"])
    df = df[df["cantidad"] > 0]

    return df

#metodo para insertar registro de salud
def insert_registro_salud(
    repo: FirebirdRepository,
    id_tipo_indicador_salud: int,
    id_enfermedad: int,
    id_diagnostico,
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
        id_enfermedad,
        id_diagnostico,
        id_municipio,
        id_fecha,
        id_grupo_etario,
        id_sexo,
        cantidad,
        id_fuente_dato
    ))

# ejecutor elt
def run_desnutricion_etl(repo: FirebirdRepository):
    if not Path(FILE_PATH).exists():
        raise FileNotFoundError(f"No existe el archivo: {FILE_PATH}")

    print(f"Procesando archivo: {FILE_PATH}")

    df = build_dataframe(FILE_PATH)

    print("Vista previa del DataFrame \"limpio\":")
    print(df.head(10))
    print(f"filas a procesar: {len(df)}")

    fuente_id = get_or_create_fuente_dato(repo)
    tipo_indicador_id = get_or_create_tipo_indicador_salud(repo, "Desnutrición aguda")
    enfermedad_id = get_or_create_enfermedad(repo, "Desnutrición aguda", "Nutricional")

    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            anio = int(row["anio"])
            departamento = row["departamento"]
            municipio = row["municipio"]
            grupo_etario = row["grupo_etario"]
            sexo_codigo, sexo_nombre = normalize_sexo(row["sexo"])
            cantidad = int(row["cantidad"])

            id_departamento = get_or_create_departamento(repo, departamento)
            id_municipio = get_or_create_municipio(repo, municipio, id_departamento)
            id_grupo_etario = get_or_create_grupo_etario(repo, grupo_etario)
            id_sexo = get_or_create_sexo(repo, sexo_codigo, sexo_nombre)
            id_fecha = get_or_create_fecha(repo, anio)

            insert_registro_salud(
                repo=repo,
                id_tipo_indicador_salud=tipo_indicador_id,
                id_enfermedad=enfermedad_id,
                id_diagnostico=None,
                id_municipio=id_municipio,
                id_fecha=id_fecha,
                id_grupo_etario=id_grupo_etario,
                id_sexo=id_sexo,
                cantidad=cantidad,
                id_fuente_dato=fuente_id
            )

            inserted += 1

            if inserted % 500 == 0:
                repo.commit()
                print(f"insertados hasta ahora: {inserted}")

        except Exception as e:
            skipped += 1
            print(f"fila omitida por error: {row.to_dict()} -> {e}")

    repo.commit()

    print(f"Insertados: {inserted}")
    print(f"Omitidos: {skipped}")