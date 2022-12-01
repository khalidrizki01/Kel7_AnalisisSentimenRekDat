from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from psaw import PushshiftAPI
import pandas as pd
import datetime as dt
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from pathlib import Path
import snscrape.modules.twitter as sntwitter
import os
from tqdm import tqdm

from utils.database import Postgres, TweetsTable
from utils.convert import ConvertDatetime
from utils.scrape import ScrapeReddit, ScrapeTwitter
from utils.clean import CleanText

POSTGRES_HOST = 'kel7_analisissentimenrekdat_postgres_1'

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay':timedelta(minutes=1), 
    'schedule_interval':'0 0 * * Mon'
}

create_table_sql_query = """
    create table if not exists tweets (
        id serial primary key,
        timestamp timestamp default current_timestamp,
        source varchar(50),
        tweet_created_at timestamp,
        tweet_clean varchar(40000)
    );
"""

def downloadFromPostgres():
    db = Postgres(
        database="airflow",
        host=POSTGRES_HOST,
        user="airflow",
        password="airflow",
        port="5432"
    )
    table_tweets = TweetsTable(db)
    
    df = table_tweets.find_all()
    df.to_csv('/opt/airflow/data/output_postgres.csv')

    db.close()

with DAG(
    dag_id = 'lgbt_reddit_twitter_download_postgres',
    default_args=default_args,
    description='DAG to download comments data from postgres',
    start_date = datetime(2022, 11, 29)
) as dag:
    initiate_postgres_table = PostgresOperator(
        sql = create_table_sql_query,
        task_id = "create_table_task",
        postgres_conn_id = "postgres_local",
    )

    download_from_postgres = PythonOperator(
        task_id = 'download_from_postgres',
        python_callable = downloadFromPostgres
    )

    initiate_postgres_table >> download_from_postgres
