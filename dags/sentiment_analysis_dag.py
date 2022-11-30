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


# from TwitterScraper import ScrapeTwitter
# from RedditScraper import ScrapeReddit
# from cleaning import CleanText
# from SentimentAnalyzer import SentimentAnalysis

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay':timedelta(minutes=1), 
    'schedule_interval':'0 7 * * *'
}

def ScrapeReddit(ti):
    api = PushshiftAPI()

    def data_prep_comments(term, subreddits, start_time, end_time, filters):
        if (len(filters) == 0):
            filters = ['id', 'author', 'created_utc',
                       'body']
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
    yesterday = today - timedelta(days = 1)

    today_year = int(today.strftime("%Y"))
    yesterday_year = int(yesterday.strftime("%Y"))
    today_month = int(today.strftime("%m"))
    yesterday_month = int(yesterday.strftime("%m"))
    today_day = int(today.strftime("%d"))
    yesterday_day = int(yesterday.strftime("%d"))

    df_comment = data_prep_comments('LGBT', 'indonesia',int(dt.datetime(yesterday_year, yesterday_month, yesterday_day).timestamp()), int(dt.datetime(today_year, today_month, today_day).timestamp()), filters=[],)
    df_comment = df_comment[['id','created', 'body']]
    df_comment.rename(columns = {'created':'DateTime', 'body':'Text'}, inplace = True)

    # df_comment = df_comment[['cleanBody_stopwords_included']]
    df_comment.to_csv(Path("/opt/airflow/data/reddit_lgbt.csv"), index=False)
    print(df_comment.loc[0,:])

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
  df_new = df[["id","Datetime","Text"]]

  df_new.dropna(how='any',axis=0)

  df_new.to_csv(Path("/opt/airflow/data/twitter_lgbt.csv"), index=False)
  print(f"twitter: {df_new.loc[0, :]}")

def CleanText():
    twitter = pd.read_csv(Path("/opt/airflow/data/twitter_lgbt.csv"))
    reddit = pd.read_csv(Path("/opt/airflow/data/reddit_lgbt.csv"))
    df = pd.concat([twitter, reddit], ignore_index=True)

    def remove_unnecessary_char(text):
        text = re.sub("\[USERNAME\]", " ", text)
        text = re.sub("\[URL\]", " ", text)
        text = re.sub("\[SENSITIVE-NO\]", " ", text)
        text = re.sub('  +', ' ', text)
        return text

    def preprocess_tweet(text):
        text = re.sub('\n',' ',text) # Remove every '\n'
        # text = re.sub('rt',' ',text) # Remove every retweet symbol
        text = re.sub('^(\@\w+ ?)+',' ',text)
        text = re.sub(r'\@\w+',' ',text) # Remove every username
        text = re.sub(r'\#\w+',' ',text) # Remove every hashtag
        text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))',' ',text) # Remove every URL
        text = re.sub('/', ' ', text)
        # text = re.sub(r'[^\w\s]', '', text)
        text = re.sub('  +', ' ', text) # Remove extra spaces
        return text
        
    def remove_nonaplhanumeric(text):
        text = re.sub('&amp;', ' ', text)
        text = re.sub('[^0-9a-zA-Z,.?!]+', ' ', text) 
        text = re.sub('  +', ' ', text)
        return text

    def fixSingkatan(text):
        text = re.sub(r'\b(aj|ae|aja)\b', 'saja', text)
        text = re.sub(r'\b(ak|gue|gw)\b','aku', text)
        text = re.sub(r'\b(belom|blm)\b', 'belum', text)
        text = re.sub(r'\b(bgt|bngt)\b', 'banget', text)
        text = re.sub(r'\b(bnyk|byk)\b', 'banyak', text)
        text = re.sub(r'\b(dlm)\b', 'dalam', text)
        text = re.sub(r'\b(dr)\b', 'dari', text)
        text = re.sub(r'\b(dg|dgn)\b','dengan',text )
        text = re.sub(r'\b(dpt|dapet)\b', 'dapat', text)
        text = re.sub(r'\b(duar+)\b', 'duar', text)
        text = re.sub(r'\b(emng|emg|emang)\b', 'memang', text)
        text = re.sub(r'\b(gt|gtu)\b', 'gitu', text)
        text = re.sub(r'\b(gatau)\b', 'tidak tau', text)
        text = re.sub(r'\b(gaada)\b', 'tidak ada', text)
        text = re.sub(r'\b(gamau)\b', 'tidak mau', text)
        text = re.sub(r'\b(gimana|gmn)\b', 'bagaimana', text)
        text = re.sub(r'\b(jgn)\b', 'jangan', text)
        text = re.sub(r'\b(jgn2|jangan2)\b', 'jangan jangan', text)
        text = re.sub(r'\b(jd|jdi)\b', 'jadi', text)
        text = re.sub(r'\b(karna|krn|krna)\b', 'karena', text)
        text = re.sub(r'\b(kyk|kek)\b', 'kayak', text)
        text = re.sub(r'\b(kl|klo|kalo)\b', 'kalau', text)
        text = re.sub(r'\b(klian)\b', 'kalian', text)
        text = re.sub(r'\b(knp)\b', 'kenapa', text)
        text = re.sub(r'\b(kpd)\b', 'kepada', text)
        text = re.sub(r'\b(lg)\b', 'lagi', text)
        text = re.sub(r'\b(lgsg)\b', 'langsung', text)
        text = re.sub(r'\b(mrk)\b', 'mereka', text)
        text = re.sub(r'\b(pd)\b', 'pada', text)
        text = re.sub(r'\b(pdhl)\b', 'padahal', text)
        text = re.sub(r'\b(pake)\b', 'pakai', text)
        text = re.sub(r'\b(org)\b', 'orang', text)
        text = re.sub(r'\b(org2)\b', 'orang orang', text)
        text = re.sub(r'\b(sbg)\b', 'sebagai', text)
        text = re.sub(r'\b(skrg)\b', 'sekarang', text)
        text = re.sub(r'\b(sm)\b', 'sama', text)
        text = re.sub(r'\b(spt)\b', 'seperti', text)
        text = re.sub(r'\b(dah|sdh|udh|udah)\b', 'sudah', text)
        text = re.sub(r'\b(tp|tpi)\b', 'tapi', text)
        text = re.sub(r'\b(tiba2|tbtb|tb2)\b', 'tiba tiba', text)
        text = re.sub(r'\b(td|tdi)\b', 'tadi', text)
        text = re.sub(r'\b(tdk|g|ga|gak|gk|engga|enggak|ngga|nggak|kaga|kagak)\b', 'tidak', text)
        text = re.sub(r'\b(trus|trs)\b', 'terus', text)
        text = re.sub(r'\b(tsb)\b', 'tersebut', text)
        text = re.sub(r'\b(ttg)\b', 'tentang', text)
        text = re.sub(r'\b(utk)\b', 'untuk', text)
        text = re.sub(r'\b(ya+h*)\b', 'ya', text)
        text = re.sub(r'\b(yg)\b', 'yang', text)
        return text

    def preprocess(text):
        text = text.lower() # Mengecilkan semua hurufnya dahulu agar lebih mudah
        text = remove_unnecessary_char(text)
        text = preprocess_tweet(text) 
        text = remove_nonaplhanumeric(text)
        return text

    df["clean_Text"] = df["Text"].apply(preprocess)
    df["clean_Text"] = df["clean_Text"].apply(fixSingkatan)
    df.to_csv(Path("/opt/airflow/data/input.csv"))
    print(f"cleaning: {df.loc[:10, 'clean_Text']}")

create_table_sql_query = """
create table if not exists tweets (
    id serial primary key,
    timestamp timestamp,
    clean_tweet varchar(40000),
    timestamp timestamp default current_timestamp
);
"""

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
    
    df = pd.read_csv('/opt/airflow/data/input.csv')
    table_tweets.insert_many_from_df(df)
    print("upload selesai")

    db.close()

with DAG(
    dag_id = 'sentiment_analysis_dag',
    default_args=default_args,
    description='DAG to run sentiment analysis on tweets and reddit comments',
    start_date = datetime(2022, 11, 29)
) as dag:
    taskScrapeReddit = PythonOperator(
        task_id = 'scrape_reddit',
        python_callable = ScrapeReddit
    )
    taskScrapeTwitter = PythonOperator(
        task_id = 'scrape_twitter',
        python_callable = ScrapeTwitter
    )
    taskClean = PythonOperator(
        task_id = 'clean_text',
        python_callable = CleanText
    )
    create_table = PostgresOperator(
        sql = create_table_sql_query,
        task_id = "create_table_task",
        postgres_conn_id = "postgres_local",
    )
    upload_csv = PythonOperator(
        task_id = 'upload_csv',
        python_callable = uploadCSV
    )
    [taskScrapeTwitter, taskScrapeReddit ] >> taskClean
    [taskClean, create_table] >> upload_csv
