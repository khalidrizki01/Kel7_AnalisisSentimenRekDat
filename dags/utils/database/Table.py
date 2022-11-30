from abc import abstractmethod
import pandas as pd

from .Postgres import Postgres


class Table(Postgres):
    """
    Berisi method untuk create dan read rows ke tabel postgres.
    Perlu diturunkan ke tabel yang lebih spesifik.
    """

    def __init__(self, db: Postgres, table_name: str, columns: list):
        self.db = db
        self.table_name = table_name
        self.columns = columns

    # READ
    def find_all(self) -> pd.DataFrame:
        cursor = self.db.conn.cursor()
        cursor.execute(f"select * from {self.table_name}")
        data = cursor.fetchall()
        cursor.close()

        df = self.get_dataframe_from_tuple(data)
        return df

    # read helper methods
    def get_dataframe_from_tuple(self, data: tuple) -> pd.DataFrame:
        df = pd.DataFrame(data, columns=['id', *self.columns, 'timestamp'])
        df = df.iloc[:, 1:]   # remove first column
        return df

    # INSERT
    # insert helper methods
    def insert_many_from_df(self, df: pd.DataFrame):
        cursor = self.db.conn.cursor()
        query = f"insert into {self.table_name} {self.get_columns_query_format()} values {self.get_values_query_format()}"
        cursor.executemany(query, self.get_values_format(df))
        self.db.conn.commit()
        cursor.close()

    def get_columns_query_format(self) -> str:
        return f"({','.join(self.columns)})"

    def get_values_query_format(self) -> str:
        n = len(self.columns)
        text = ''
        for i in range(n):
            text += "%s"
            if(i < n-1):
                text += ","
        text = f"({text})"
        return text

    # NEED TO BE IMPLEMENTED
    @abstractmethod
    def get_values_format(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def insert_many(self, *args: pd.Series):
        pass
