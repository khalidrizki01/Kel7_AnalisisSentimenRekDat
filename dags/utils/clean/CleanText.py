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


def CleanText():
    twitter = pd.read_csv(Path("/opt/airflow/data/twitter_lgbt.csv"))
    twitter['source'] = "twitter"
    reddit = pd.read_csv(Path("/opt/airflow/data/reddit_lgbt.csv"))
    reddit['source'] = "reddit"
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
    rows_to_drop = df[df['clean_Text']==''].index
    df.drop(rows_to_drop, inplace=True)
    df.dropna()
    df.to_csv(Path("/opt/airflow/data/output_clean_text.csv"))
    print(f"cleaning: {df.loc[:10, 'clean_Text']}")
