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

import pandas as pd
from utils.database import Postgres, TweetsTable
from utils.convert import ConvertDatetime

# from TwitterScraper import ScrapeTwitter
#from RedditScraper import ScrapeReddit
# from SentimentAnalyzer import SentimentAnalysis

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay':timedelta(minutes=1), 
    'schedule_interval':'0 7 * * *'
}

def ScrapeReddit(ti):
    api = PushshiftAPI()

    def data_prep_comments(term, start_time, end_time, filters):
        if (len(filters) == 0):
            filters = ['id', 'author', 'created_utc',
                    'body', 'subreddit']
                    #We set by default some usefull columns 

        comments = list(api.search_comments(
            q=term,                 #Subreddit we want to audit
            ##subreddit = subreddits,
            after=start_time,       #Start date
            before=end_time,        #End date
            filter=filters))        #Column names we want to retrieve
            ##limit=limit))           #Max number of comments
    
        return pd.DataFrame(comments) #Return dataframe for analysis

    today = date.today()
    yesterday = today - timedelta(days = 1)

    today_year = int(today.strftime("%Y"))
    yesterday_year = int(yesterday.strftime("%Y"))
    today_month = int(today.strftime("%m"))
    yesterday_month = int(yesterday.strftime("%m"))
    today_day = int(today.strftime("%d"))
    yesterday_day = int(yesterday.strftime("%d"))

    df_comment = data_prep_comments('LGBT', int(dt.datetime(yesterday_year, yesterday_month, yesterday_day).timestamp()), int(dt.datetime(today_year, today_month, today_day).timestamp()), filters=[],)

    def remove_nonaplhanumeric(text):
        text = re.sub('&amp;', ' ', text)
        text = re.sub('[^0-9a-zA-Z,.?!]+', ' ', text) 
        text = re.sub('  +', ' ', text)
        return text

    def remove_unnecessary_char(text):
        text = re.sub("\[USERNAME\]", " ", text)
        text = re.sub("\[URL\]", " ", text)
        text = re.sub("\[SENSITIVE-NO\]", " ", text)
        text = re.sub('  +', ' ', text)
        return text

    def preprocess(text):
        text = text.lower() # Mengecilkan semua hurufnya dahulu agar lebih mudah
        text = remove_unnecessary_char(text) 
        text = remove_nonaplhanumeric(text)
        return text

    df_comment["cleanBody_stopwords_included"] = df_comment["body"].apply(preprocess)
    
    try:
        print(df_comment.loc[0, 'cleanBody_stopwords_included'])
        df_comment.to_csv(Path("/opt/airflow/data/reddit_lgbt.csv"))
        ti.xcom_push(key='reddit_csv', value=df_comment.loc[0, 'cleanBody_stopwords_included'])
        return True
    except OSError as e:
        print(e)
        return False
    #print(df_comment.iloc[0,:])
    # df_new = df_comment.iloc[:10, :]
    # df_new.to_csv()
    # df_comment.to_csv()
    

def getCSV(ti):
    reddit = pd.read_csv(Path("/opt/airflow/data/reddit_lgbt.csv"))
    print(f"getcsv: {reddit.loc[0, 'cleanBody_stopwords_included']}")


def uploadCSV():
    # buat koneksi ke db
    db = Postgres(
        database="airflow",
        host="analisissentimenrekdat_postgres_1",
        user="airflow",
        password="airflow",
        port="5432"
    )
    table_tweets = TweetsTable(db)
    
    df = pd.read_csv('/opt/airflow/data/output.csv')
    table_tweets.insert_many_from_df(df)
    print("upload selesai")

    db.close()

def getCSVPostgres():
    db = Postgres(
        database="airflow",
        host="analisissentimenrekdat_postgres_1",
        user="airflow",
        password="airflow",
        port="5432"
    )
    table_tweets = TweetsTable(db)

    df = table_tweets.find_all()
    df.to_csv('/opt/airflow/data/output2.csv')

with DAG(
    dag_id = 'trial_db',
    default_args=default_args,
    description='DAG to run sentiment analysis on tweets and reddit comments',
    start_date = datetime(2022, 11, 29)
) as dag:
    # taskScrapeTwitter = PythonOperator(
    #     task_id='ScrapeTwitter', 
    #     python_callable = 'ScrapeTwitter'
    # )
    taskScrapeReddit = PythonOperator(
        task_id = 'scrape_reddit',
        python_callable = ScrapeReddit
    )
    taskGetCSV = PythonOperator(
        task_id = 'get_csv',
        python_callable = getCSV
    )

    create_table_sql_query = """
    create table if not exists tweets (
        id serial primary key,
        date date,
        time time,
        clean_tweet varchar(40000),
        source varchar(50),
        timestamp timestamp default current_timestamp
    );
    """

    create_table = PostgresOperator(
        sql = create_table_sql_query,
        task_id = "create_table_task",
        postgres_conn_id = "postgres_local",
    )

    upload_csv = PythonOperator(
        task_id = 'upload_csv',
        python_callable = uploadCSV
    )

    get_csv_postgres = PythonOperator(
        task_id = 'get_csv_postgres',
        python_callable = getCSVPostgres
    )

    create_table >> get_csv_postgres >> upload_csv >> taskScrapeReddit >> taskGetCSV