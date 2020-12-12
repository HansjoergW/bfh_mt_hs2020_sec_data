from yahoo_historical import Fetcher
import pandas as pd
import os
from typing import List
from multiprocessing import Pool
import time

all_data_local_folder = "./data/"
stock_data_folder = "D:/data/stocks/sec/"


def load_data() -> pd.DataFrame:
    df = pd.read_csv(all_data_local_folder + "complete.csv")

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



def save(df: pd.DataFrame, filename:str, name:str):
    df.to_csv(filename, sep=',', encoding='utf-8', index=False)


def get_filename(ticker:str):
    return stock_data_folder + ticker[0] + "/" + ticker + ".csv"


def download_and_store(ticker: str):
    filename = get_filename(ticker)
    if os.path.isfile(filename):
        #print("skipping: ", ticker)
        return

    #print("loading: ", ticker)
    try:
        data = downloadTicker(ticker, 2010)
    except Exception as ex:
        print("failed: ", ticker, " - ", str(ex))
        return

    if data.shape[0] > 200:
        save(data, filename, ticker)
    else:
        print("too short: ", data.shape[0], " - ", ticker)


def parallel_download(tickers: List[str]):
    start = time.time()
    pool = Pool(15)
    pool.map_async(download_and_store, tickers)
    pool.close()
    pool.join()
    print("duration: ", time.time() - start)


def serial_download(tickers: List[str]):
    for ticker in tickers:
        download_and_store(ticker)


if __name__ == '__main__':
    tickers = list(load_data().ticker.sort_values().unique())
    print(len(tickers))
    create_dirs(tickers)

    #parallel_download(tickers)
    serial_download(tickers)

    # check for missing files
    for ticker in tickers:
        if os.path.isfile(get_filename(ticker)) == False:
            print(ticker,end=",")