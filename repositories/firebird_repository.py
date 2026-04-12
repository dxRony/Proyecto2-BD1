class FirebirdRepository:
    def __init__(self, connection):
        self.conn = connection
        self.cursor = self.conn.cursor()

    def execute(self, query, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    def executemany(self, query, data):
        self.cursor.executemany(query, data)

    def fetch_one(self):
        return self.cursor.fetchone()

    def fetch_all(self):
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()