import random
from repositories.firebird_repository import FirebirdRepository


def get_ids(repo, table):
    repo.execute(f"SELECT id FROM {table}")
    return [r[0] for r in repo.fetch_all()]


def run_faker_hechos_etl(repo: FirebirdRepository):
    print("Iniciando ETL faker de vínculos")


    # obtener datos de base
    personas = get_ids(repo, "persona")
    hechos = get_ids(repo, "hecho_delictivo")
    fechas = get_ids(repo, "fecha")
    estados = get_ids(repo, "estado_caso")
    tipos_denuncia = get_ids(repo, "tipo_denuncia")
    involucramientos = get_ids(repo, "involucramiento")
    relaciones = get_ids(repo, "relacion_agresor")

    if not personas or not hechos:
        print("No hay personas o hechos para vincular")
        return

    # separando personas por tipo
    repo.execute("""
        SELECT p.id, p.edad, s.nombre
        FROM persona p
        JOIN sexo s ON s.id = p.id_sexo
    """)
    data = repo.fetch_all()

    menores = [r[0] for r in data if r[1] is not None and r[1] < 18]
    hombres_adultos = [r[0] for r in data if r[1] is not None and r[1] >= 18 and "hombre" in r[2].lower()]
    mujeres_adultas = [r[0] for r in data if r[1] is not None and r[1] >= 18 and "mujer" in r[2].lower()]

    print(f"Menores: {len(menores)}")
    print(f"Hombres adultos: {len(hombres_adultos)}")
    print(f"Mujeres adultas: {len(mujeres_adultas)}")

    #denuncias
    print("generando denuncias...")

    for i in range(1200):
        persona_id = random.choice(menores + mujeres_adultas)
        hecho_id = random.choice(hechos)
        fecha_id = random.choice(fechas)

        repo.execute("""
            INSERT INTO denuncia (id_fecha, id_hecho_delictivo, id_persona, id_estado_caso, id_tipo_denuncia)
            VALUES (?, ?, ?, ?, ?)
        """, (
            fecha_id,
            hecho_id,
            persona_id,
            random.choice(estados) if estados else None,
            random.choice(tipos_denuncia) if tipos_denuncia else None
        ))

        if i % 200 == 0:
            print(f"Denuncias insertadas: {i}")

    #involucramientos
    print("generando involucramientos...")

    for i in range(2400):
        hecho_id = random.choice(hechos)

        # alternar víctima/agresor
        if i % 2 == 0:
            persona_id = random.choice(menores + mujeres_adultas)
        else:
            persona_id = random.choice(hombres_adultos)

        repo.execute("""
            INSERT INTO involucramiento_hecho (
                id_persona,
                id_hecho_delictivo,
                id_involucramiento
            )
            VALUES (?, ?, ?)
        """, (
            persona_id,
            hecho_id,
            random.choice(involucramientos) if involucramientos else None
        ))

        if i % 400 == 0:
            print(f"Involucramientos insertados: {i}")


    # violencia intrafamiliar
    print("creando casos de violencia intrafamiliar...")

    for i in range(600):
        victima = random.choice(menores + mujeres_adultas)
        agresor = random.choice(hombres_adultos)

        if victima == agresor:
            continue

        hecho_id = random.choice(hechos)

        repo.execute("""
            INSERT INTO violencia_intrafamiliar (
                id_persona_victima,
                id_persona_agresor,
                id_hecho_delictivo,
                id_relacion_agresor,
                reiteracion_denuncia
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            victima,
            agresor,
            hecho_id,
            random.choice(relaciones) if relaciones else None,
            random.choice([True, False])
        ))

        if i % 100 == 0:
            print(f"VIF insertadas: {i}")

    repo.commit()

    print("faker de hechos finalizado")