import random

from repositories.firebird_repository import FirebirdRepository

#metodo para obtener ids de una tabla
def get_ids(repo: FirebirdRepository, table: str):
    repo.execute(f"SELECT id FROM {table}")
    return [r[0] for r in repo.fetch_all()]

#metodo para obtener menores de edad con su edad
def get_menores(repo: FirebirdRepository):
    repo.execute("""
        SELECT p.id, p.edad
        FROM persona p
        WHERE p.edad IS NOT NULL
          AND p.edad < 18
    """)
    return repo.fetch_all()

#metodo para elegir grupo etario compatible con la edad
def pick_grupo_etario_for_minor(repo: FirebirdRepository, edad: int):
    repo.execute("""
        SELECT id, edad_min, edad_max
        FROM grupo_etario
        WHERE edad_min IS NOT NULL
          AND edad_max IS NOT NULL
    """)
    rows = repo.fetch_all()

    compatibles = []
    for grupo_id, edad_min, edad_max in rows:
        if edad_min <= edad <= edad_max:
            compatibles.append(grupo_id)

    if compatibles:
        return random.choice(compatibles)

    return None

#metodo para elegir escolaridad compatible con la edad
def pick_escolaridad_for_minor(repo: FirebirdRepository, edad: int):
    repo.execute("SELECT id, nombre FROM escolaridad")
    rows = repo.fetch_all()

    if not rows:
        return None

    if edad <= 12:
        keywords = ["primaria", "ninguna", "ignorado"]
    else:
        keywords = ["primaria", "básico", "basico", "diversificado", "ninguna", "ignorado"]

    filtered = []
    for row_id, nombre in rows:
        nombre_norm = str(nombre).strip().lower()
        if any(k in nombre_norm for k in keywords):
            filtered.append(row_id)

    if filtered:
        return random.choice(filtered)

    return random.choice(rows)[0]

#metodo para obtener o crear un sector economico, retornando su id
def seed_sector_economico(repo: FirebirdRepository):
    sectores_base = [
        "Agricultura",
        "Industria",
        "Comercio",
        "Servicios",
        "Trabajo doméstico"
    ]

    for nombre in sectores_base:
        repo.execute("""
            SELECT id
            FROM sector_economico
            WHERE LOWER(nombre) = LOWER(?)
        """, (nombre,))
        row = repo.fetch_one()

        if not row:
            repo.execute("""
                INSERT INTO sector_economico (nombre)
                VALUES (?)
            """, (nombre,))

#ejecutor etl
def run_faker_trabajo_infantil_etl(repo: FirebirdRepository, total: int = 450):
    print("Iniciando ETL faker de trabajo infantil")
    seed_sector_economico(repo)
    #obteniendo menores de edad disponibles
    menores = get_menores(repo)
    if not menores:
        print("No hay personas menores de edad disponibles")
        return
    #obteniendo ids de tablas relacionadas
    areas = get_ids(repo, "area_geografica")
    fuentes = get_ids(repo, "fuente_dato")
    sectores = get_ids(repo, "sector_economico")
    tipos_trabajo = get_ids(repo, "tipo_trabajo")
    condiciones = get_ids(repo, "condicion_actividad")
    motivos = get_ids(repo, "motivo_trabajo")
    apoyos = get_ids(repo, "apoyo_hogar")

    insertados = 0
    #insertando registros de trabajo infantil
    for _ in range(total):
        persona_id, edad = random.choice(menores)

        id_grupo_etario = pick_grupo_etario_for_minor(repo, edad)
        id_escolaridad = pick_escolaridad_for_minor(repo, edad)

        asiste_escuela = random.random() < 0.70
        indicador_domestico = random.random() < 0.35

        horas_trabajo = round(random.uniform(2, 8), 2)
        ingreso = round(random.uniform(15, 125), 2)

        repo.execute("""
            INSERT INTO trabajo_infantil (
                id_persona,
                id_area_geografica,
                id_fuente_dato,
                id_grupo_etario,
                id_escolaridad,
                asiste_escuela,
                id_sector_economico,
                id_tipo_trabajo,
                id_condicion_actividad,
                horas_trabajo,
                ingreso,
                id_motivo_trabajo,
                id_apoyo_hogar,
                indicador_domestico
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            persona_id,
            random.choice(areas) if areas else None,
            random.choice(fuentes) if fuentes else None,
            id_grupo_etario,
            id_escolaridad,
            asiste_escuela,
            random.choice(sectores) if sectores else None,
            random.choice(tipos_trabajo) if tipos_trabajo else None,
            random.choice(condiciones) if condiciones else None,
            horas_trabajo,
            ingreso,
            random.choice(motivos) if motivos else None,
            random.choice(apoyos) if apoyos else None,
            indicador_domestico
        ))

        insertados += 1
        if insertados % 100 == 0:
            print(f"Trabajo infantil insertado: {insertados}")

    repo.commit()
    print(f"Total trabajo infantil insertado: {insertados}")