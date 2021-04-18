###############
### Imports ###
###############

from psaw import PushshiftAPI
import datetime as dt
import pandas as pd
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import nltk

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from textblob import TextBlob

###################
### Pull Titles ###
###################

def pull_titles(start_date, end_date, subreddit):
    api = PushshiftAPI()
    one_day_delta = pd.Timedelta("1 days")
    curr_date = start_date
    dfs = []
    # Collect titles for days 1 to 8 in the given month / year
    while (curr_date < end_date):
        # Start and end dates
        next_date = curr_date + one_day_delta
        start_epoch = int(curr_date.timestamp())
        end_epoch = int(next_date.timestamp())
        print(curr_date, next_date)

        # Fetch the submissions from reddit
        raw_dataset = list(api.search_submissions(
            after=start_epoch,
            before=end_epoch,
            subreddit=subreddit,
            filter=['url','author', 'title', 'subreddit'],
            limit=1000
        ))

        # Convert to df
        df = pd.DataFrame([submission.d_ for submission in raw_dataset])
        print(len(df))
        dfs.append(df)

        # Sleep to avoid rate limits
        curr_date = next_date
        time.sleep(5)
    
    # Concat all dfs together
    reddit_titles_df = pd.concat(dfs)
    reddit_titles_df['date'] = pd.to_datetime(reddit_titles_df['created_utc'] , unit='s')
    reddit_titles_df = reddit_titles_df.sort_values(by=['date'])
    reddit_titles_df = reddit_titles_df[['date', 'title']]
    return reddit_titles_df

############################
### Get Sentiment Scores ###
############################

def get_sentiment_scores(reddit_titles_df):
    # Create the spark session
    spark_session = SparkSession \
        .builder \
        .appName('Python Spark SQL basic example') \
        .getOrCreate()

    reddit_titles_df_sp = spark_session.createDataFrame(reddit_titles_df)

    # Use PySpark to calcualte snetiment scores
    sentiment = udf(lambda x: TextBlob(x).sentiment[0])
    spark_session.udf.register('sentiment', sentiment)
    reddit_titles_df_sp = reddit_titles_df_sp.withColumn('compund', sentiment('title').cast('float'))
    reddit_titles_df = reddit_titles_df_sp.toPandas()

    # Aggregate
    sentiment_scores_df = reddit_titles_df[['compound', 'date']]
    sentiment_scores_df = sentiment_scores_df.resample('3h', on='date').mean()
    sentiment_scores_df = sentiment_scores_df.reset_index()

    # Add the offset date so we can join for predictions
    time_delta = pd.Timedelta(3, unit='h')
    sentiment_scores_df['offset_date'] = sentiment_scores_df['date'].apply(lambda date: date + time_delta)

    return sentiment_scores_df
