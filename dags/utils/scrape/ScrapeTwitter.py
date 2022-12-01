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

def ScrapeTwitter():
  def scrape_tweets(query, max_tweets=-1,output_path="./scraper/output/" ): 
      output_path = os.path.join(output_path,dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")+"-"+str(query)+".csv")
      
      tweets_list = []
      try:
          for i,tweet in tqdm(enumerate(sntwitter.TwitterSearchScraper(query).get_items())):
              if max_tweets != -1 and i >= int(max_tweets):
                  break
              tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.username])
      except KeyboardInterrupt:
          print("Scraping berhenti atas permintaan pengguna")
      df = pd.DataFrame(tweets_list, columns=['Datetime', 'id', 'Text', 'Username'])
      #df.to_csv(output_path, index=False)
      return df


  today = date.today()
  yesterday = today - timedelta(days = 1)
  today_new = today.strftime("%Y-%m-%d")
  yesterday_new = yesterday.strftime("%Y-%m-%d")

  df = scrape_tweets("lgbt OR lgbtq OR onelove lang:id since:"+yesterday_new+" until:"+today_new, -1, "/content/")

  df = df.drop_duplicates()
  df = df.dropna(how='any',axis=0)
  df_new = df[["Datetime","Text"]]

  df_new.dropna(how='any',axis=0)

  df_new.to_csv(Path("/opt/airflow/data/twitter_lgbt.csv"), index=False)
  print(f"twitter: {df_new.loc[0, :]}")