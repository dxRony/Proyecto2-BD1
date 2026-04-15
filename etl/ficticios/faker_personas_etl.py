from faker import Faker
import random

from repositories.firebird_repository import FirebirdRepository

fake = Faker("es_ES")


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def get_random_id_from_table(repo: FirebirdRepository, table_name: str):
    repo.execute(f"SELECT id FROM {table_name}")
    rows = repo.fetch_all()
    if not rows:
        return None
    return random.choice(rows)[0]


def get_or_create_sexo(repo: FirebirdRepository, codigo: str, nombre: str) -> int:
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
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def get_or_create_condicion_edad(repo: FirebirdRepository, codigo: str, nombre: str) -> int:
    repo.execute("""
        SELECT id
        FROM condicion_edad
        WHERE codigo = ?
    """, (codigo,))
    row = repo.fetch_one()
    if row:
        return row[0]

    repo.execute("""
        INSERT INTO condicion_edad (codigo, nombre)
        VALUES (?, ?)
        RETURNING id
    """, (codigo, nombre))
    return repo.fetch_one()[0]


def create_persona(repo: FirebirdRepository, id_sexo: int, edad: int | None = None) -> int:
    repo.execute("""
        INSERT INTO persona (id_sexo, edad)
        VALUES (?, ?)
        RETURNING id
    """, (id_sexo, edad))
    return repo.fetch_one()[0]


def create_detalle_persona(
    repo: FirebirdRepository,
    id_persona: int,
    id_estado_conyugal: int | None,
    id_nacionalidad: int | None,
    id_condicion_edad: int | None,
    id_escolaridad: int | None,
    id_grupo_etnico: int | None,
    id_orientacion: int | None,
):
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
        id_nacionalidad,
        id_condicion_edad,
        id_escolaridad,
        id_grupo_etnico,
        id_orientacion
    ))


def pick_escolaridad_for_age(repo: FirebirdRepository, edad: int):
    repo.execute("SELECT id, nombre FROM escolaridad")
    rows = repo.fetch_all()
    if not rows:
        return None

    if edad < 13:
        keywords = ["preprimaria", "primaria", "ninguna", "ignorado"]
    elif edad < 18:
        keywords = ["primaria", "básico", "basico", "diversificado", "ninguna", "ignorado"]
    else:
        keywords = ["primaria", "básico", "basico", "diversificado", "universitario", "ninguna", "ignorado"]

    filtered = []
    for row_id, nombre in rows:
        nombre_norm = normalize_text(nombre).lower()
        if any(k in nombre_norm for k in keywords):
            filtered.append(row_id)

    if filtered:
        return random.choice(filtered)

    return random.choice(rows)[0]


def run_faker_personas_etl(
    repo: FirebirdRepository,
    total_menores: int = 1500,
    total_hombres_adultos: int = 1500,
    total_mujeres_adultas: int = 1500
):
    print("Iniciando ETL faker de personas")

    id_sexo_hombre = get_or_create_sexo(repo, "H", "Hombre")
    id_sexo_mujer = get_or_create_sexo(repo, "M", "Mujer")

    id_condicion_menor = get_or_create_condicion_edad(repo, "MENOR", "Menor de edad")
    id_condicion_mayor = get_or_create_condicion_edad(repo, "MAYOR", "Mayor de edad")

    insertados = 0

    # menores de edad: 750 niñas y 750 niños
    for i in range(total_menores):
        sexo_id = id_sexo_mujer if i < (total_menores // 2) else id_sexo_hombre
        edad = random.randint(8, 17)

        persona_id = create_persona(repo, sexo_id, edad)

        create_detalle_persona(
            repo=repo,
            id_persona=persona_id,
            id_estado_conyugal=None,
            id_nacionalidad=get_random_id_from_table(repo, "nacionalidad"),
            id_condicion_edad=id_condicion_menor,
            id_escolaridad=pick_escolaridad_for_age(repo, edad),
            id_grupo_etnico=get_random_id_from_table(repo, "grupo_etnico"),
            id_orientacion=None
        )

        insertados += 1
        if insertados % 500 == 0:
            print(f"Personas insertadas: {insertados}")

    # hombres adultos
    for _ in range(total_hombres_adultos):
        edad = random.randint(18, 65)

        persona_id = create_persona(repo, id_sexo_hombre, edad)

        create_detalle_persona(
            repo=repo,
            id_persona=persona_id,
            id_estado_conyugal=get_random_id_from_table(repo, "estado_conyugal"),
            id_nacionalidad=get_random_id_from_table(repo, "nacionalidad"),
            id_condicion_edad=id_condicion_mayor,
            id_escolaridad=pick_escolaridad_for_age(repo, edad),
            id_grupo_etnico=get_random_id_from_table(repo, "grupo_etnico"),
            id_orientacion=None
        )

        insertados += 1
        if insertados % 500 == 0:
            print(f"Personas insertadas: {insertados}")

    # mujeres adultas
    for _ in range(total_mujeres_adultas):
        edad = random.randint(18, 65)

        persona_id = create_persona(repo, id_sexo_mujer, edad)

        create_detalle_persona(
            repo=repo,
            id_persona=persona_id,
            id_estado_conyugal=get_random_id_from_table(repo, "estado_conyugal"),
            id_nacionalidad=get_random_id_from_table(repo, "nacionalidad"),
            id_condicion_edad=id_condicion_mayor,
            id_escolaridad=pick_escolaridad_for_age(repo, edad),
            id_grupo_etnico=get_random_id_from_table(repo, "grupo_etnico"),
            id_orientacion=None
        )

        insertados += 1
        if insertados % 500 == 0:
            print(f"Personas insertadas: {insertados}")

    repo.commit()
    print(f"Total personas faker insertadas: {insertados}")