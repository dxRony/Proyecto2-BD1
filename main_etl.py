import argparse

from config.db import get_connection, test_connection
from repositories.firebird_repository import FirebirdRepository

from etl.salud.desnutricion_etl import run_desnutricion_etl
from etl.salud.retardo_desarrollo_etl import run_retardo_desarrollo_etl
from etl.salud.salud_etl import run_salud_etl

#enfermedades transmitidas por vectores
VECTOR_MODULES = {
    "dengue": (
        "Salud/Enfermedades transmitidas por vectores/enfermedades-transmitidas-por-vectores-2012-al-2024-dengue.csv",
        "Dengue",
        "Enfermedades transmitidas por vectores"
    ),
    "dengue_grave": (
        "Salud/Enfermedades transmitidas por vectores/enfermedades-transmitidas-por-vectores-2012-al-2024-dengue-grave.csv",
        "Dengue grave",
        "Enfermedades transmitidas por vectores"
    ),
    "malaria": (
        "Salud/Enfermedades transmitidas por vectores/enfermedades-transmitidas-por-vectores-2012-al-2024-malaria.csv",
        "Malaria",
        "Enfermedades transmitidas por vectores"
    ),
    "chagas": (
        "Salud/Enfermedades transmitidas por vectores/enfermedades-transmitidas-por-vectores-2012-al-2024-chagas.csv",
        "Chagas",
        "Enfermedades transmitidas por vectores"
    ),
    "zika": (
        "Salud/Enfermedades transmitidas por vectores/enfermedades-transmitidas-por-vectores-2012-al-2024-zika.csv",
        "Zika",
        "Enfermedades transmitidas por vectores"
    ),
    "chikungunya": (
        "Salud/Enfermedades transmitidas por vectores/enfermedades-transmitidas-por-vectores-2012-al-2024-chikungunya.csv",
        "Chikungunya",
        "Enfermedades transmitidas por vectores"
    ),
}


def run_catalogs(repo: FirebirdRepository):
    print("Cargando catálogos base")


def run_module(module_name: str, repo: FirebirdRepository):
    print(f"Ejecutando módulo: {module_name}")

    if module_name == "desnutricion":
        run_desnutricion_etl(repo)
        return

    if module_name == "retardo_desarrollo":
        run_retardo_desarrollo_etl(repo)
        return

    if module_name in VECTOR_MODULES:
        file_path, enfermedad, tipo_indicador = VECTOR_MODULES[module_name]
        run_salud_etl(repo, file_path, enfermedad, tipo_indicador)
        return

    print("Módulo no reconocido")


def main():
    parser = argparse.ArgumentParser(description="ETL Proyecto BD1")
    parser.add_argument("--test-connection", action="store_true")
    parser.add_argument("--catalogs", action="store_true")
    parser.add_argument("--module", type=str)
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    if args.test_connection:
        test_connection()
        return

    conn = get_connection()
    repo = FirebirdRepository(conn)

    try:
        if args.catalogs:
            run_catalogs(repo)

        if args.module:
            run_module(args.module, repo)

        print("ETL finalizado correctamente")

    except Exception as e:
        repo.rollback()
        print("Error en ETL:", e)

    finally:
        repo.close()


if __name__ == "__main__":
    main()