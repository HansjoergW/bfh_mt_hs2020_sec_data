# imports
import time  # used to measure execution time
from typing import List
import pandas as pd
import glob
from multiprocessing import Pool


stock_data_folder     = "D:/data_mt/08_stock_data/"
all_data_local_folder = "./data/"

join_group = ["cik", "ticker", "adsh", "period", "filed", "form", "fp"]


def get_data_files() -> List[str]:
    return glob.glob(stock_data_folder + "**/*_2.csv", recursive=True)


def read_stockdata(filename: str) -> pd.DataFrame:
    df = pd.read_csv(filename, header=0, sep=",", encoding='utf-8')
    df.Date = pd.to_datetime(df.Date)
    return df


def read_additional_info() -> pd.DataFrame:
    return pd.read_csv(stock_data_folder + "08_add_ticker_info.csv", sep=',', encoding='utf-8', header=0)


def get_ticker_from_filename(filename: str) -> str:
    return filename[len(stock_data_folder) + 2: -6]


def norm_historical_data(df: pd.DataFrame, shares_outstanding: float):
    lastClose = df[-1:].Close.values[0]
    df['close_norm'] = df.Close / lastClose
    df['high_norm'] = (df.High - df.Close) / df.Close
    df['low_norm'] = (df.Low - df.Close) / df.Close
    df['open_norm'] = (df.Open - df.Close) / df.Close
    df['Volume'] = df['Volume'].astype(float)
    df['volume_norm'] = df.Volume / shares_outstanding


def create_features_from_historical_data(df: pd.DataFrame):
    df['close_chg'] = 0.0

    close_data = df.Close.to_numpy()
    close_change = ((close_data[1:] - close_data[:-1]) / close_data[:-1])

    df.loc[df.index[1:], 'close_chg'] = close_change

    df['volume_chg'] = 0.0

    volume_data = df.Volume.to_numpy()
    volume_change = ((volume_data[1:] - volume_data[:-1]) / volume_data[:-1])

    df.loc[df.index[1:], 'volume_chg'] = volume_change

    df['day_of_week'] = df.Date.dt.dayofweek / 7
    df['day_of_month'] = df.Date.dt.day / 31
    df['day_of_year'] = df.Date.dt.dayofyear / 365
    df['week_of_year'] = df.Date.dt.weekofyear / 52
    df['month_of_year'] = df.Date.dt.month / 12


def process_stock_data(filename: str, df_add_info: pd.DataFrame):
    ticker = get_ticker_from_filename(filename)

    print("process: ", ticker)
    df_stock = read_stockdata(filename)
    df_stock_add_info = df_add_info[df_add_info.ticker == ticker]

    sharesOutstanding = df_stock_add_info[-1:].sharesOutstanding.values[0]
    norm_historical_data(df_stock, sharesOutstanding)
    create_features_from_historical_data(df_stock)

    df_stock['ticker'] = ticker # add a ticker column
    df_stock.to_csv(stock_data_folder + ticker[0] + "/" + ticker + "_processed.csv", sep=',', encoding='utf-8',
                    index=False)


df_add_info = read_additional_info()


def call_process_stock(filname: str):
    process_stock_data(filname, df_add_info)


if __name__ == '__main__':
    files = get_data_files()

    start = time.time()
    pool = Pool(8)
    pool.map(call_process_stock, files)
    pool.close()
    pool.join()

    print("duration: ", time.time() - start)
