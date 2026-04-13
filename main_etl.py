import argparse

from config.db import get_connection, test_connection
from repositories.firebird_repository import FirebirdRepository
from etl.salud.desnutricion_etl import run_desnutricion_etl
from etl.salud.retardo_desarrollo_etl import run_retardo_desarrollo_etl
from etl.salud.salud_etl import run_salud_etl
from etl.salud.cronicas_etl import run_cronicas_etl
from etl.salud.maternal_etl import run_maternal_etl
from etl.violencia.quejas_mineduc_etl import run_quejas_mineduc_etl
from etl.violencia.discriminacion_etl import run_discriminacion_etl
from etl.violencia.violencia_intrafamiliar_etl import run_violencia_intrafamiliar_etl
from etl.violencia.faltas_judiciales_etl import run_faltas_judiciales_etl
from etl.violencia.pnc_victimas_etl import run_pnc_victimas_etl
from etl.violencia.pnc_detenidos_etl import run_pnc_detenidos_etl
from etl.violencia.oj_sentenciados_etl import run_oj_sentenciados_etl
from etl.violencia.necropsias_etl import run_necropsias_etl
from etl.violencia.exhumaciones_etl import run_exhumaciones_etl
from etl.violencia.agraviados_etl import run_agraviados_etl
from etl.violencia.sindicatos_etl import run_sindicatos_etl
from etl.violencia.evaluacion_inacif_etl import run_inacif_etl


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

#enfermedades cronicas
CRONICAS_MODULES = {
    "cronicas_2020": "Salud/Enfermedades_Cronicas_2020-2024/mec-2020-departamento-municipio.csv",
    "cronicas_2021": "Salud/Enfermedades_Cronicas_2020-2024/mec-2021-departamento-municipio.csv",
    "cronicas_2022": "Salud/Enfermedades_Cronicas_2020-2024/mec-2022-departamento-municipio.csv",
    "cronicas_2023": "Salud/Enfermedades_Cronicas_2020-2024/mec-2023-departamento-municipio.csv",
    "cronicas_2024": "Salud/Enfermedades_Cronicas_2020-2024/mec-2024-departamento-municipio.csv",
}

VIOLENCIA_INTRAFAMILIAR_MODULES = {
    "violencia_intrafamiliar_2023": (
        "Violencia/Violencia intrafamiliar/2023/2024052300200h2NuX0RKNUd61gZu7ox4PQXgDUDgLC9Y.xlsx",
        "Violencia/Violencia intrafamiliar/2023/Diccionario/2024052300613QDinUvuRa9GjopyXaTuNMXc3gd6Jq1Q1.xlsx",
        "Violencia intrafamiliar 2023"
    ),
    "violencia_intrafamiliar_2024": (
        "Violencia/Violencia intrafamiliar/2024/base-de-datos-violencia-intrafamiliar-ano-2024_v3.xlsx",
        "Violencia/Violencia intrafamiliar/2024/diccionario-de-variables-violencia-intrafamiliar.xlsx",
        "Violencia intrafamiliar 2024"
    ),
}

FALTAS_JUDICIALES_MODULES = {
    "faltas_judiciales": (
        "Violencia/Faltas judiciales/20240524231759eHmz6DmFKboNQ5Y3OlqNkbi9izmXULaP.xlsx",
        "Faltas judiciales"
    ),
}

PNC_VICTIMAS_MODULES = {
    "pnc_victimas": (
        "Violencia/Hechos-Delicitivos/PNC -Victimas/20240528163848NcFYbAN6bA92ZKRG7mLINYvyZoVmXEDA.xlsx",
        "PNC Víctimas"
    ),
}

PNC_DETENIDOS_MODULES = {
    "pnc_detenidos": (
        "Violencia/Hechos-Delicitivos/PNC - Detenciados/20240528163735e7G5K5EZOlGPCBiH4ROtQRgfm5sTifW1.xlsx",
        "PNC Detenidos"
    ),
}

OJ_SENTENCIADOS_MODULES = {
    "oj_sentenciados": (
        "Violencia/Hechos-Delicitivos/Organismo judical - Sentenciados/20240528163614FaXwFKh8NYNiFivgBo98JEbaVMRUhaFG.xlsx",
        "OJ Sentenciados"
    ),
}

NECROPSIAS_MODULES = {
    "necropsias": (
        "Violencia/Hechos-Delicitivos/Necropsias/20240528163210puPW7O9wJalS7I9yToxkpQLgLwNElAny.xlsx",
        "Necropsias"
    ),
}

EXHUMACIONES_MODULES = {
    "exhumaciones": (
        "Violencia/Hechos-Delicitivos/Exhumaciones/20240528163020xWnoL8XOFWtEdaj6FS3AtmzgpEXyVyB8.xlsx",
        "Exhumaciones"
    ),
}

AGRAVIADOS_MODULES = {
    "agraviados": (
        "Violencia/Hechos-Delicitivos/Agraviados/20240528163342pWf6BcBWj8taVS3Q3mRKxgDsvwPejgH8.xlsx",
        "Agraviados MP"
    ),
}

SINDICATOS_MODULES = {
    "sindicatos": (
        "Violencia/Hechos-Delicitivos/Sindicatos/20240528163458VEdqlo5oBmhO5cvKTQhYRYj2D05gxCla.xlsx",
        "Sindicatos MP"
    ),
}

INACIF_MODULES = {
    "inacif": "Violencia/Hechos-Delicitivos/Evaluacion Medicos - INACIF/20240528162820xWnoL8XOFWtEdaj6FS3AtmzgpEXyVyB8.xlsx"
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

    if module_name in CRONICAS_MODULES:
        run_cronicas_etl(
            repo,
            CRONICAS_MODULES[module_name],
            f"Enfermedades crónicas {module_name[-4:]}"
        )
        return

    if module_name == "neonatal":
        run_cronicas_etl(
            repo,
            "Salud/Morbilidad Grupo Materno Infantil/morbilidad-neonatal-2012-al-2024.csv",
            "Morbilidad neonatal"
        )
        return

    if module_name == "maternal":
        run_maternal_etl(
            repo,
            "Salud/Morbilidad Grupo Materno Infantil/morbilidad-materna-2012-al-2024.csv"
        )
        return

    if module_name == "quejas_mineduc":
        run_quejas_mineduc_etl(repo)
        return

    if module_name == "discriminacion":
        run_discriminacion_etl(repo)
        return

    if module_name in VIOLENCIA_INTRAFAMILIAR_MODULES:
        file_path, dict_path, dataset_name = VIOLENCIA_INTRAFAMILIAR_MODULES[module_name]
        run_violencia_intrafamiliar_etl(repo, file_path, dict_path, dataset_name)
        return

    if module_name in FALTAS_JUDICIALES_MODULES:
        file_path, dataset_name = FALTAS_JUDICIALES_MODULES[module_name]
        run_faltas_judiciales_etl(repo, file_path, dataset_name)
        return

    if module_name in PNC_VICTIMAS_MODULES:
        file_path, dataset_name = PNC_VICTIMAS_MODULES[module_name]
        run_pnc_victimas_etl(repo, file_path, dataset_name)
        return
    
    if module_name in PNC_DETENIDOS_MODULES:
        file_path, dataset_name = PNC_DETENIDOS_MODULES[module_name]
        run_pnc_detenidos_etl(repo, file_path, dataset_name)
        return

    if module_name in OJ_SENTENCIADOS_MODULES:
        file_path, dataset_name = OJ_SENTENCIADOS_MODULES[module_name]
        run_oj_sentenciados_etl(repo, file_path, dataset_name)
        return

    if module_name in NECROPSIAS_MODULES:
        file_path, dataset_name = NECROPSIAS_MODULES[module_name]
        run_necropsias_etl(repo, file_path, dataset_name)
        return
    
    if module_name in EXHUMACIONES_MODULES:
        file_path, dataset_name = EXHUMACIONES_MODULES[module_name]
        run_exhumaciones_etl(repo, file_path, dataset_name)
        return

    if module_name in AGRAVIADOS_MODULES:
        file_path, dataset_name = AGRAVIADOS_MODULES[module_name]
        run_agraviados_etl(repo, file_path, dataset_name)
        return

    if module_name in SINDICATOS_MODULES:
        file_path, dataset_name = SINDICATOS_MODULES[module_name]
        run_sindicatos_etl(repo, file_path, dataset_name)
        return
    
    if module_name in INACIF_MODULES:
        run_inacif_etl(repo, INACIF_MODULES[module_name])
        return

    print("Modulo no reconocido")

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