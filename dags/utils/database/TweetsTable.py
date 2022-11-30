import pandas as pd

from .Postgres import Postgres
from .Table import Table


class TweetsTable(Table):
    def __init__(
        self,
        db: Postgres,

    ):
        super().__init__(
            db,
            table_name="tweets",
            columns=['date', 'time', 'clean_tweet', 'source']
        )

    def insert_many(
        self,
        source: str,
        date: pd.Series,
        time: pd.Series,
        clean_tweet: pd.Series
    ):
        df = self.create_dataframe(source, date, time, clean_tweet)
        self.insert_many_from_df(df)

    def create_dataframe(self, source, date, time, clean_tweet) -> pd.DataFrame:
        df = pd.DataFrame({
            'date': date,
            'time': time,
            'clean_tweet': clean_tweet,
        })
        df['source'] = source
        return df

    def get_values_format(self, df: pd.DataFrame):
        values = []
        nrows = df.shape[0]
        for i in range(nrows):
            row = (
                df['date'][i],
                df['time'][i],
                df['clean_tweet'][i],
                df['source'][i]
            )
            values.append(row)
        return tuple(values)
