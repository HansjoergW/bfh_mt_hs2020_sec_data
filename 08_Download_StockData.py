from yahoo_historical import Fetcher
import yfinance as yf
import pandas as pd
import os
from typing import List, Dict
from multiprocessing import Pool
import time

all_data_local_folder = "./data/"
stock_data_folder = "D:/data/stocks/sec/"


def load_data() -> pd.DataFrame:
    df = pd.read_csv(all_data_local_folder + "07_all_features_complete_corrected.csv")

    df.period = pd.to_datetime(df.period)
    df.filed = pd.to_datetime(df.filed)

    df.sort_values('period', inplace = True)
    df.reset_index(inplace = True)
    return df

def ensure_dir(file_path:str):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        print(file_path)
        os.makedirs(directory)


def create_dirs(tickers:List[str]):
    ensure_dir(stock_data_folder)
    for ticker in tickers:
        ensure_dir(stock_data_folder + ticker[0] + "/")


def downloadTicker(stock:str, startyear:int)->pd.DataFrame:
    stockFetcher = Fetcher(stock,[startyear,1,1],[2020,12,10])
    stock_df = stockFetcher.get_historical()
    return stock_df


def downloadTicker_yf(stock:str, startyear:int)->pd.DataFrame:
    stock_df = yf.download(stock,start=str(startyear)+"-01-01",end="2020-12-10", progress=False, auto_adjust=True)
    return stock_df


def save(df: pd.DataFrame, filename:str):
    df.to_csv(filename, sep=',', encoding='utf-8', index=True)


def get_filename(ticker:str):
    return stock_data_folder + ticker[0] + "/" + ticker + "_2.csv"


def download_and_store(ticker: str):
    filename = get_filename(ticker)
    if os.path.isfile(filename):
        print("skipping: ", ticker)
        return

    print("loading: ", ticker)
    try:
        data = downloadTicker_yf(ticker, 2010)
    except Exception as ex:
        print("failed: ", ticker, " - ", str(ex))
        return

    if data.shape[0] > 200:
        save(data, filename)
    else:
        print("too short: ", data.shape[0], " - ", ticker)


def parallel_download(tickers: List[str]):
    start = time.time()
    pool = Pool(20)
    pool.map_async(download_and_store, tickers)
    pool.close()
    pool.join()
    print("duration: ", time.time() - start)


def serial_download(tickers: List[str]):
    for ticker in tickers:
        download_and_store(ticker)


def download_tickers(tickers: List[str]):
    parallel_download(tickers)
    #serial_download(tickers)

    # check for missing files
    for ticker in tickers:
        if os.path.isfile(get_filename(ticker)) == False:
            print(ticker,end=",")


def get_add_data(ticker:str) -> Dict[str,str]:

    try:
        print(ticker)
        info = yf.Ticker(ticker)
        return {'ticker'            : ticker,
                'sector'            : info.info['sector'],
                'industry'          : info.info['industry'],
                'marketCap'         : info.info['marketCap'],
                'sharesOutstanding' : info.info['sharesOutstanding'],
                'message'           : None}
    except Exception as ex:
        msg = str(ex)
        if "404" in msg:
            return None

        return {'ticker'            : ticker,
                'sector'            : None,
                'industry'          : None,
                'marketCap'         : None,
                'sharesOutstanding' : None,
                'message'           : msg}


def parallel_download_add_info(tickers: List[str]):
    filename = all_data_local_folder + "08_add_ticker_info.csv"
    current = pd.read_csv(filename, sep=',', encoding='utf-8', header=0)

    current_tickers = current.ticker.to_list()
    tickers = list(set(tickers) - set(current_tickers))

    data = pd.DataFrame(columns=['ticker', 'sector', 'industry','marketCap', 'sharesOutstanding','message'],)

    start = time.time()
    pool = Pool(20)
    list_infos = pool.map(get_add_data, tickers)
    pool.close()
    pool.join()

    entries = [x for x in list_infos if x is not None]
    print(len(entries))
    print("duration: ", time.time() - start)

    for entry in entries:
        data = data.append(entry, ignore_index=True)

    data = pd.concat([current, data])

    print(data.shape)
    print("duration: ", time.time() - start)

    data.to_csv(all_data_local_folder + "08_add_ticker_info.csv", sep=',', encoding='utf-8', index=False)


def download_add_info(tickers: List[str]):
    data = pd.DataFrame(columns=['ticker', 'sector', 'industry','marketCap', 'sharesOutstanding'])

    for ticker in tickers:
        try:
            info = yf.Ticker(ticker)
            data = data.append({'ticker'            : ticker,
                                'sector'            : info.info['sector'],
                                'industry'          : info.info['industry'],
                                'marketCap'         : info.info['marketCap'],
                                'sharesOutstanding' : info.info['sharesOutstanding']},

                               ignore_index=True)
        except:
            pass
    print(data.shape)



if __name__ == '__main__':
    tickers = list(load_data().ticker.sort_values().unique())
    print(len(tickers))
    create_dirs(tickers)

    #download_tickers(tickers)
    parallel_download_add_info(tickers)
    #download_add_info(tickers[:10])



