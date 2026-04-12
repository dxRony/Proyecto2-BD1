import argparse
from config.db import get_connection, test_connection
from repositories.firebird_repository import FirebirdRepository


def run_catalogs(repo):
    print("Cargando catalogos base")


def run_module(module_name, repo):
    print(f"Ejecutando módulo: {module_name}")

    if module_name == "quejas_mineduc":
        print("aun no implementado")
    else:
        print("modulo no reconocido")


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