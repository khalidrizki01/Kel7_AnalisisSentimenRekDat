import psycopg2


class Postgres:
    def __init__(self, database="airflowdb", host="localhost", user="postgres", password="root", port="5432"):
        self.conn = psycopg2.connect(
            database=database,
            host=host,
            user=user,
            password=password,
            port=port
        )

    # def __del__(self):
    #     self.conn.close()

    def close(self):
        self.conn.close()
