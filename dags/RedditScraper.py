from psaw import PushshiftAPI
import pandas as pd
import datetime as dt
from datetime import date
from datetime import timedelta
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

def ScrapeReddit():
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



    # eng_stopwords = stopwords.words('english')

    def remove_nonaplhanumeric(text):
        text = re.sub('&amp;', ' ', text)
        text = re.sub('[^0-9a-zA-Z,.?!]+', ' ', text) 
        text = re.sub('  +', ' ', text)
        return text

    # def remove_stopword(text):
    #     text = re.sub('[.,!?]','',text)
    #     text = ' '.join(['' if word in eng_stopwords else word for word in text.split(' ')]) #Mengganti stopword dengan ''
    #     text = re.sub('  +', ' ', text)
    #     text = text.title()
    #     text = text.strip() #Menghapus spasi atau newline di awal dan akhir kalimat yang tidak diperlukan
    #     return text

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
    print(df_comment.iloc[0,:])
    print(df_comment.iloc[0,:])
    df_comment.to_csv('reddit_lgbtq.csv')