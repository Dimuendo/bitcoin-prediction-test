import gen_data_fxns.get_bitcoin_prices as gbp
import gen_data_fxns.reddit_headlines as rh
import datetime as dt

def main():
    # Get all data
    print('Getting Bitcoin Prices')
    bitcoin_prices_df = gbp.get_prices('./data/bitcoin_prices.csv')
    print('Getting Reddit Titles')
    start_date = dt.datetime(2021, 3, 15)
    end_date = dt.datetime(2021, 4, 14)
    reddit_titles_df = rh.pull_titles(start_date, end_date, 'bitcoin')
    print('Calculating Sentiment Scores')
    sentiment_scores_df = rh.get_sentiment_scores(reddit_titles_df)

    # Output data to parquets
    bitcoin_prices_df.to_parquet('./data/bitcoin_prices.parquet.gzip', compression='gzip')
    reddit_titles_df.to_parquet('./data/reddit_titles.parquet.gzip', compression='gzip')
    sentiment_scores_df.to_parquet('./data/sentiment_scores.parquet.gzip', compression='gzip')

if __name__ == "__main__":
    main()