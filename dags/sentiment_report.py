from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
from datetime import datetime, timedelta, date
import tqdm
from pathlib import Path
from utils.database import Postgres, TweetsTable

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

def assign_sentiment():
    from transformers import pipeline
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    from matplotlib import pyplot as plt
    import os.path

    pretrained_name = "sahri/indonesiasentiment"

    sentiment_classifier = pipeline(
        "text-classification",
        model=pretrained_name,
        # tokenizer=AutoTokenizer.from_pretrained(pretrained_name)
        device=0
    )


    """## Load Integrated-Cleaned Data

    # ### Load From Uploaded CSV File
    # """

    # df = pd.read_csv(Path("/opt/airflow/data/input.csv"))

    # for i in range (len(df)):
    #     if len(df.clean_Text[i].split()) >= 182: # maximum inputnya agar bisa bekerja adalah sejumlah 182
    #         df.clean_Text[i] = ''
    # df_clean = df['clean_Text']

    """### Load from SQL Database"""
    import psycopg2
    import pandas as pd
    from sqlalchemy import create_engine

    # # Create an engine instance
    # engine = create_engine('postgresql+psycopg2://test:@127.0.0.1', pool_recycle=3600);

    # # Connect to PostgreSQL server
    # dbConnection = engine.connect();

    # df = pd.read_sql("select * from \"<Nama Tabel>\"", dbConnection);
    df = pd.read_csv(Path('/opt/airflow/data/output2.csv'))
    for i in range (len(df)):
        if len(df.clean_tweet[i].split()) >= 182: # maximum inputnya agar bisa bekerja adalah sejumlah 182
            df.clean_tweet[i] = ''
    df_clean = df['clean_tweet']

    pd.set_option('display.expand_frame_repr', False);


    sentiment_list = []

    text_list = df_clean.tolist()

    for i in tqdm(range(len(df))): 
    
        if(text_list[i] == ''):
            result = sentiment_classifier('neutral')[0]
            sentiment_list.append(result["label"])
        else:
            result = sentiment_classifier(text_list[i])[0]
            sentiment_list.append(result["label"])

    print(sentiment_list[:5])

    # print(indexToDrop)
    textLengthToDrop = []

    for i in indexToDrop:
        textLengthToDrop.append(len(text_list[i].split()))
    print(f"minimum length classifier doesn't work: {min(textLengthToDrop)}")

    """## Load Sentiment Analysis Result to DataFrame"""

    df["sentiment"] = sentiment_list

    print(f'banyak sentimen: {df["sentiment"].value_counts()}')
    df.to_csv(Path(f"/opt/airflow/data/LabeledSentimentAnalysis-{datetime.now()}.csv"), index=false)

def generatePieChart():
    df = pd.read_csv(Path(f"/opt/airflow/data/LabeledSentimentAnalysis-{datetime.now()}.csv"))
    colors = ['#ff0000','#4e24ff','#eeff38']

    piechart = df.groupby(["sentiment"])["date"].count().plot.pie(autopct="%.1f%%", figsize=(7,7),  colors=colors, startangle=90) # Kali ini ga usah pakai explode aja
    piechart.figure.patch.set_facecolor('white')
    piechart.set_title('Analisis Sentimen')
    piechart.yaxis.set_visible(False)
    # piechart.figure.patch.set_alpha(0)

    """### Save Pie Chart Picture"""

    # piechart.figure.savefig("/content/drive/Shareddrives/Tugas Data Engineering/SentimentAnalysis.png")
    # path = "/content/drive"
    fn = "LgbtSentimentPieChart.jpg"
    piechart.figure.savefig(f"{path}/{fn}")

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay':timedelta(minutes=1), 
    'schedule_interval':'0 7 * * *'
}

with DAG(
    dag_id = 'sentiment_report',
    default_args=default_args,
    description='DAG to run sentiment reports',
    start_date = datetime(2022, 11, 29)
) as dag:
    taskGetCSVFromPostgres = PythonOperator(
        task_id = 'get_csv_from_postgres',
        python_callable = getCSVPostgres
    )
    taskAssignSentiment = PythonOperator(
        task_id = 'assign_sentiment',
        python_callable = assign_sentiment
    )
    taskGeneratePieChart = PythonOperator(
        task_id = 'generate_pie_chart',
        python_callable = generatePieChart
    )
    taskGetCSVFromPostgres >> taskAssignSentiment >> taskGeneratePieChart