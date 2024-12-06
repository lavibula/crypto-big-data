import yfinance as yf

def fetch_yahoo_data(start_date, end_date, symbol):
    """
    Fetch historical data for a given asset from Yahoo Finance and format columns.

    Parameters:
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.
        symbol (str): The financial asset symbol (e.g., 'BTC-USD').

    Returns:
        pd.DataFrame: DataFrame containing the asset's historical data with renamed columns.
    """

    commodity_to_symbol = {
        "GOLD": "GC=F",        # Gold Futures
        "SILVER": "SI=F",      # Silver Futures
        "PLATINUM": "PL=F",    # Platinum Futures
        "COPPER": "HG=F",      # Copper Futures
        "CRUDE OIL": "CL=F",   # Crude Oil Futures
        "NATURAL GAS": "NG=F", # Natural Gas Futures
        "PALLADIUM": "PA=F"    # Palladium Futures
    }
    if symbol in commodity_to_symbol.keys():
        converted_symbol=commodity_to_symbol[symbol]
    else:
        converted_symbol=symbol

    ticker = yf.Ticker(converted_symbol)
    hist_data = ticker.history(start=start_date, end=end_date)

    # Reset index and add a 'BASE' column
    hist_data.reset_index(inplace=True)
    hist_data['BASE'] = symbol.split('-')[0]  # Extract base (e.g., 'BTC' from 'BTC-USD')

    # Rename columns to match the exact desired names
    hist_data = hist_data.rename(columns={
        'Date': 'DATE',
        'Open': 'OPEN',
        'High': 'HIGH',
        'Low': 'LOW',
        'Close': 'CLOSE',
        'Volume': 'VOLUME'
    })

    # Select and reorder columns to match the specified format
    hist_data = hist_data[['BASE', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]
    hist_data = hist_data.iloc[::-1].reset_index(drop=True)
    return hist_data


if __name__=='__main__':
    # response=requests.get(url='https://data-api.cryptocompare.com/spot/v1/markets/instruments')

    # result=response.json()
    # market_instruments={}
    # for i in ['coinbase','kraken']:
    #     curr_market=result['Data'][i]['instruments']  
    #     num_usd=[x.replace('-USD','') for x in list(curr_market.keys()) if x.endswith('-USD')]
    #     market_instruments[i]=num_usd
    #     print(f"{i} market has {len(num_usd)} USD active instruments")
    # kraken_only_instruments = set(market_instruments['kraken']) - set(market_instruments['coinbase'])
    # # Các đồng tiền cần lấy dữ liệu
    # crypto_ids = [('coinbase', coin) for coin in market_instruments['coinbase']]+[('kraken',coin) for coin in kraken_only_instruments]
 
    # Example usage
    start_date = '2023-01-01'
    end_date = '2024-11-18'
    symbol = 'GOLD'
    df = fetch_yahoo_data(start_date, end_date, symbol)
    print(df)