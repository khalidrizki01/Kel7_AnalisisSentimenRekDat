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
    'schedule_interval':'0 7 * * *'
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

def uploadToPostgres():
    # buat koneksi ke db
    db = Postgres(
        database="airflow",
        host=POSTGRES_HOST,
        user="airflow",
        password="airflow",
        port="5432"
    )
    table_tweets = TweetsTable(db)
    
    df = pd.read_csv('/opt/airflow/data/output_clean_text.csv')
    table_tweets.insert_many(
        source=df['source'],
        tweet_created_at=df['Datetime'],
        tweet_clean=df['clean_Text']
    )
    print("upload selesai")

    db.close()


with DAG(
    dag_id = 'lgbt_reddit_twitter_etl',
    default_args=default_args,
    description='DAG to run sentiment analysis on tweets and reddit comments',
    start_date = datetime(2022, 11, 29)
) as dag:

    scrape_reddit = PythonOperator(
        task_id = 'scrape_reddit',
        python_callable = ScrapeReddit
    )

    scrape_twitter = PythonOperator(
        task_id = 'scrape_twitter',
        python_callable = ScrapeTwitter
    )

    clean_text = PythonOperator(
        task_id = 'clean_text',
        python_callable = CleanText
    )

    initiate_postgres_table = PostgresOperator(
        sql = create_table_sql_query,
        task_id = "create_table_task",
        postgres_conn_id = "postgres_local",
    )

    upload_to_postgres = PythonOperator(
        task_id = 'upload_to_postgres',
        python_callable = uploadToPostgres
    )

    [scrape_twitter, scrape_reddit] >> clean_text
    [clean_text, initiate_postgres_table] >> upload_to_postgres
