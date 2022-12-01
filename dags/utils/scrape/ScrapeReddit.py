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

def ScrapeReddit():
    api = PushshiftAPI()

    def data_prep_comments(term, subreddits, start_time, end_time, filters):
        if (len(filters) == 0):
            filters = ['id', 'author', 'created_utc', 'body']
                       #We set by default some usefull columns 

        comments = list(api.search_comments(
            q=term,                 #Subreddit we want to audit
            subreddit = subreddits,
            after=start_time,       #Start date
            before=end_time,        #End date
            filter=filters))        #Column names we want to retrieve
            ##limit=limit))          #Max number of comments
  
        return pd.DataFrame(comments) #Return dataframe for analysis

    today = date.today()
    yesterday = today - timedelta(days = 2)

    today_year = int(today.strftime("%Y"))
    yesterday_year = int(yesterday.strftime("%Y"))
    today_month = int(today.strftime("%m"))
    yesterday_month = int(yesterday.strftime("%m"))
    today_day = int(today.strftime("%d"))
    yesterday_day = int(yesterday.strftime("%d"))

    df_comment = data_prep_comments('LGBT', 'indonesia',int(dt.datetime(yesterday_year, yesterday_month, yesterday_day).timestamp()), int(dt.datetime(today_year, today_month, today_day).timestamp()), filters=[],)
    df_comment['created_utc'] = pd.to_datetime(df_comment['created_utc'], 
                                       unit = 's')
    df_comment = df_comment[['created_utc', 'body']]
    df_comment.rename(columns = {'created_utc':'Datetime', 'body':'Text'}, inplace = True)

    # df_comment = df_comment[['cleanBody_stopwords_included']]
    df_comment.to_csv(Path("/opt/airflow/data/reddit_lgbt.csv"), index=False)
    print(df_comment.loc[0,:])
