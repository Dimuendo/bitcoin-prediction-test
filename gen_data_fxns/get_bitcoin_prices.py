###############
### Imports ###
###############

import pandas as pd

##################
### Get Prices ###
##################

def get_prices(input):
    bitcoin_prices_df = pd.read_csv(input)
    bitcoin_prices_df['date'] = pd.to_datetime(
        bitcoin_prices_df['time'], unit='ms'
    )
    bitcoin_prices_df['price_diff'] = bitcoin_prices_df['close'].diff()
    bitcoin_prices_df = bitcoin_prices_df[['date', 'price_diff']]
    bitcoin_prices_df = bitcoin_prices_df.reset_index(drop=True)
    return bitcoin_prices_df
