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
            columns=['source', 'tweet_created_at', 'tweet_clean']
        )

    def insert_many(
        self,
        source: pd.Series,
        tweet_created_at: pd.Series,
        tweet_clean: pd.Series
    ):
        df = self.create_dataframe(source, tweet_created_at, tweet_clean)
        self.insert_many_from_df(df)

    def create_dataframe(self, source, tweet_created_at, tweet_clean) -> pd.DataFrame:
        df = pd.DataFrame({
            'source': source,
            'tweet_created_at': tweet_created_at,
            'tweet_clean': tweet_clean
        })
        # df['source'] = source
        return df

    def get_values_format(self, df: pd.DataFrame):
        values = []
        nrows = df.shape[0]
        for i in range(nrows):
            row = (
                df['source'][i],
                df['tweet_created_at'][i],
                df['tweet_clean'][i]
            )
            values.append(row)
        return tuple(values)
