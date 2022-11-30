from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from psaw import PushshiftAPI
import pandas as pd
import datetime as dt
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from pathlib import Path

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
    print(f"getcsv: {reddit.iloc[0, 'cleanBody_stopwords_included']}")
    

with DAG(
    dag_id = 'sentiment_analysis_dag',
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
    taskScrapeReddit >> taskGetCSV
