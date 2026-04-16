import fdb

DB_PATH = "/var/lib/firebird/data/indicadores_gt.fdb"
USER = "SYSDBA"
PASSWORD = "masterkey"
OUTPUT_FILE = "inserts.sql"

TABLES = [
    "departamento",
    "municipio",
    "area_geografica",
    "franja_horaria",
    "fecha",
    "sexo",
    "zona",
    "estado_conyugal",
    "grupo_etario",
    "categoria_delito",
    "tipo_fallo",
    "fuente_dato",
    "involucramiento",
    "tipo_ley",
    "titulo_ley",
    "capitulo_ley",
    "delito",
    "nacionalidad",
    "condicion_edad",
    "causa_muerte",
    "clasificacion_evaluacion",
    "estado_caso",
    "escolaridad",
    "grupo_etnico",
    "orientacion",
    "tipo_denuncia",
    "tipo_hecho_delictivo",
    "sede",
    "tipo_atencion",
    "tipo_delito_atendido",
    "despacho_judicial",
    "sector_economico",
    "condicion_actividad",
    "motivo_trabajo",
    "apoyo_hogar",
    "tipo_trabajo",
    "tipo_agresion",
    "tipo_medida",
    "relacion_agresor",
    "tipo_falta",
    "condicion_alfabetismo",
    "estado_ebriedad",
    "ocupacion",
    "tipo_discriminacion",
    "comunidad_linguistica",
    "diagnostico_cie10",
    "tipo_indicador_salud",
    "enfermedad",
    "persona",
    "detalle_persona",
    "hecho_delictivo",
    "hecho_delictivo_mujer_estadistica",
    "delito_contra_vida_mujer_estadistica",
    "denuncia",
    "involucramiento_hecho",
    "proceso_judicial",
    "sentencia",
    "medida_seguridad",
    "medidas_seguridad_estadistica",
    "queja_mineduc_estadistica",
    "violencia_intrafamiliar",
    "falta_judicial",
    "caso_discriminacion",
    "exhumacion",
    "necropsia",
    "evaluacion_medica",
    "atencion_victima",
    "sentencias_mp_estadistica",
    "sentencias_oj_estadistica",
    "trabajo_infantil",
    "registro_salud",
]

def sql_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)

def get_columns(cursor, table_name):
    cursor.execute(f"SELECT FIRST 1 * FROM {table_name}")
    return [desc[0] for desc in cursor.description]

def export_table(cursor, file, table_name):
    columns = get_columns(cursor, table_name)
    col_list = ", ".join(columns)

    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    for row in rows:
        values = ", ".join(sql_value(v) for v in row)
        file.write(f"INSERT INTO {table_name} ({col_list}) VALUES ({values});\n")

    file.write("\n")

def main():
    conn = fdb.connect(
        dsn=f"localhost:{DB_PATH}",
        user=USER,
        password=PASSWORD,
        charset="UTF8"
    )
    cursor = conn.cursor()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"CONNECT '{DB_PATH}' USER '{USER}' PASSWORD '{PASSWORD}';\n\n")

        for table in TABLES:
            print(f"Exportando {table}...")
            export_table(cursor, f, table)

        f.write("COMMIT;\n")

    cursor.close()
    conn.close()
    print(f"Archivo generado: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()