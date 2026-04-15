from utils.normalizers import normalize_text

class FirebirdRepository:
    def __init__(self, connection):
        self.conn = connection
        self.cursor = self.conn.cursor()

    # metodo para ejecutar una consulta SQL con parms opcionales
    def execute(self, query, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    # metodo para ejecutar una consulta de inserciin multiple con una lista de datos
    def executemany(self, query, data):
        self.cursor.executemany(query, data)

    # metodo para obtener una fila del resultado de la consulta
    def fetch_one(self):
        return self.cursor.fetchone()

    # metodo para obtener todas las filas del resultado de la consulta
    def fetch_all(self):
        return self.cursor.fetchall()

    # metodo para confirmar los cambios en la base de datos
    def commit(self):
        if self.conn:
            self.conn.commit()

    # metodo para revierte los cambios en caso de error
    def rollback(self):
        try:
            if self.conn and getattr(self.conn, "main_transaction", None):
                self.conn.rollback()
        except Exception:
            pass

    # metodo para cerrar el cursor y la coneccion de manera segura
    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
        except Exception:
            pass

        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass

    # metodo para obtener o crear una fuente de dato por institucion, dataset y tipo de fuente
    def get_or_create_fuente_dato(self, institucion: str, dataset: str, tipo_fuente: str) -> int:
        self.execute("""
            SELECT id
            FROM fuente_dato
            WHERE LOWER(institucion) = LOWER(?)
              AND LOWER(dataset) = LOWER(?)
              AND LOWER(tipo_fuente) = LOWER(?)
        """, (institucion, dataset, tipo_fuente))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("""
            INSERT INTO fuente_dato (institucion, dataset, tipo_fuente)
            VALUES (?, ?, ?)
            RETURNING id
        """, (institucion, dataset, tipo_fuente))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    #metodo para obtener o crear un tipo de indicador de salud por nombre
    def get_or_create_tipo_indicador_salud(self, nombre: str) -> int:
        self.execute("""
            SELECT id
            FROM tipo_indicador_salud
            WHERE LOWER(nombre) = LOWER(?)
        """, (nombre,))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("""
            INSERT INTO tipo_indicador_salud (nombre)
            VALUES (?)
            RETURNING id
        """, (nombre,))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    # metodo para obtener o crear una enfermedad por nombre y tipo
    def get_or_create_enfermedad(self, nombre: str, tipo: str) -> int:
        self.execute("""
            SELECT id
            FROM enfermedad
            WHERE LOWER(nombre) = LOWER(?)
              AND LOWER(tipo) = LOWER(?)
        """, (nombre, tipo))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("""
            INSERT INTO enfermedad (nombre, tipo)
            VALUES (?, ?)
            RETURNING id
        """, (nombre, tipo))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    # metodo para obtener o crear un departamento con codigo generado automaticamautente
    def get_or_create_departamento(self, nombre: str) -> int:
        self.execute("""
            SELECT id
            FROM departamento
            WHERE LOWER(nombre) = LOWER(?)
        """, (nombre,))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("SELECT COUNT(*) FROM departamento")
        count_row = self.fetch_one()
        correlativo = (count_row[0] or 0) + 1
        codigo_tmp = f"D{correlativo:03d}"

        self.execute("""
            INSERT INTO departamento (codigo, nombre)
            VALUES (?, ?)
            RETURNING id
        """, (codigo_tmp, nombre))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    # metodo para obtener o crear un municipio con codiego generado basado en el departamento
    def get_or_create_municipio(self, nombre: str, id_departamento: int) -> int:
        self.execute("""
            SELECT id
            FROM municipio
            WHERE LOWER(nombre) = LOWER(?)
              AND id_departamento = ?
        """, (nombre, id_departamento))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("""
            SELECT COUNT(*)
            FROM municipio
            WHERE id_departamento = ?
        """, (id_departamento,))
        count_row = self.fetch_one()
        correlativo = (count_row[0] or 0) + 1

        codigo_tmp = f"M{id_departamento:02d}{correlativo:03d}"

        self.execute("""
            INSERT INTO municipio (codigo, nombre, id_departamento)
            VALUES (?, ?, ?)
            RETURNING id
        """, (codigo_tmp, nombre, id_departamento))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    # metodo para obtener o crear un grupo etario con codigo generado
    def get_or_create_grupo_etario(self, nombre: str) -> int:
        self.execute("""
            SELECT id
            FROM grupo_etario
            WHERE LOWER(nombre) = LOWER(?)
        """, (nombre,))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("SELECT COUNT(*) FROM grupo_etario")
        count_row = self.fetch_one()
        correlativo = (count_row[0] or 0) + 1
        codigo_tmp = f"G{correlativo:03d}"

        self.execute("""
            INSERT INTO grupo_etario (codigo, nombre, edad_min, edad_max, tipo_grupo)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
        """, (codigo_tmp, nombre, None, None, "salud"))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    # metodo para obtener o crear un registro de sexo/genero
    def get_or_create_sexo(self, codigo: str, nombre: str) -> int:
        self.execute("""
            SELECT id
            FROM sexo
            WHERE LOWER(codigo) = LOWER(?)
               OR LOWER(nombre) = LOWER(?)
        """, (codigo, nombre))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("""
            INSERT INTO sexo (codigo, nombre)
            VALUES (?, ?)
            RETURNING id
        """, (codigo, nombre))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    # metodo para obtener o crear una fecha para un ano especifico
    def get_or_create_fecha(self, anio: int) -> int:
        self.execute("""
            SELECT id
            FROM fecha
            WHERE anio = ? AND mes = 1 AND dia = 1
        """, (anio,))
        row = self.fetch_one()

        if row:
            return row[0]

        self.execute("""
            INSERT INTO fecha (fecha, anio, mes, nombre_mes, dia, dia_semana)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (f"{anio}-01-01", anio, 1, "Enero", 1, "Lunes"))
        new_id = self.fetch_one()[0]
        self.commit()
        return new_id

    # metodo para insertar un registro de salud con todos los campos relacionados
    def insert_registro_salud(
        self,
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
        self.execute("""
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