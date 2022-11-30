from psaw import PushshiftAPI
import pandas as pd
import datetime as dt
from datetime import date
from datetime import timedelta
from pathlib import Path

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
    df_comment = df_comment[['created', 'body']]
    df_comment.rename(columns = {'created':'DateTime', 'body':'Text'}, inplace = True)

    # df_comment = df_comment[['cleanBody_stopwords_included']]
    df_comment.to_csv(Path("/opt/airflow/data/reddit_lgbt.csv"))
    print(df_comment.loc[0,:])

    # idn_stopwords = stopwords.words('indonesian')
    # idn_stopwords.extend(
    #     ['ae','ah','aing','aj','aja','ak','anjir','ayo','banget','belom','bener2','bgt','biar','bikin','blm','btw','bnyk','byk','cuman','d','dah','dapet','deh','deket',
    #     'dg','dgn','disana','dll','doang','dpt','dr','drpd','duar','duarr','duarrr','duluan','e', 'eh','emang','emg','engga','g','ga','gak','gaada','gamau','gara',
    #     'gara2','gatau','gegara','gimana','gini','gtu','gitu','gk','gmn','gt','gue','gw','hah', 'haha','jd','jdi','jg','jgn','kagak','ka','kaga','kah','kak','kalo',
    #     'kali','karna','kau','kayak','kayaknya','kek','kl','klian','klo','knp','kpd','ku','krn','krna','kyk','la','lg','lgsg','lha','lho','lo','loh','lu','ma','mah','mending',
    #     'min','mo','mrk','msh','na','nder','nge','ngga','nggak','ni','nih','ntar','nya','org','pakai','pake','pd','pdhl','salah','sama2','sampe','sbg','sdh',
    #     'sebenernya','sat','set','satset','si','sih','situ','skrg','sm','spt','tau','tau2','tbtb','tb2','td','tdk','tiba2','tp','tpi','ttg','trus','trs','tsb',
    #     'tu','tuh','udah','udh','utk','xixi','w','wes','wkwk','wkwkw','wkwkwk','wkwkwkwk','wow','x','y','yg','ya','yaa','yaaa','yah','ygy','yo','yuk'])
    # idn_stopwords.extend(
    #     ['ra','mbak','mas','kui','ki','po','ora','iki','karo','opo','sek','sik','arep','og','wae','ngono','tenan','ben','iso','kudu','sing','nek','dadi','pa','iku','saiki'])

    # def remove_nonaplhanumeric(text):
    #     text = re.sub('&amp;', ' ', text)
    #     text = re.sub('[^0-9a-zA-Z,.?!]+', ' ', text) 
    #     text = re.sub('  +', ' ', text)
    #     return text

    # def remove_unnecessary_char(text):
    #     text = re.sub("\[USERNAME\]", " ", text)
    #     text = re.sub("\[URL\]", " ", text)
    #     text = re.sub("\[SENSITIVE-NO\]", " ", text)
    #     text = re.sub('  +', ' ', text)
    #     return text

    # def preprocess(text):
    #     text = text.lower() # Mengecilkan semua hurufnya dahulu agar lebih mudah
    #     text = remove_unnecessary_char(text) 
    #     text = remove_nonaplhanumeric(text)
    #     return text

    # df_comment["cleanBody_stopwords_included"] = df_comment["body"].apply(preprocess)
    
    # try:
    #     print(df_comment.loc[0, 'cleanBody_stopwords_included'])
    #     df_comment.to_csv(Path("/opt/airflow/data/reddit_lgbt.csv"))
    #     ti.xcom_push(key='reddit_csv', value=df_comment.loc[0, 'cleanBody_stopwords_included'])
    #     return True
    # except OSError as e:
    #     print(e)
    #     return False
    # #print(df_comment.iloc[0,:])
    # # df_new = df_comment.iloc[:10, :]
    # # df_new.to_csv()
    # # df_comment.to_csv()