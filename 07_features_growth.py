from multiprocessing import Pool
import pandas as pd
from typing import List

mycounter = 0

def calculate_growth(all_df: pd.DataFrame, grouped_df: pd.DataFrame, data_cols:List[str], new_cols_prefix:List[str]):

    for data_col, new_col_prefix in zip(data_cols, new_cols_prefix):
        col_name_p = new_col_prefix + "_p"
        col_name_n = new_col_prefix + "_n"

        all_df.loc[grouped_df.index[0], col_name_p] = 1.0
        all_df.loc[grouped_df.index[0], col_name_n] = 0.0

        is_positiv_arr = (grouped_df[data_col] > 0.0).to_numpy()
        is_negativ_arr = (grouped_df[data_col] < 0.0).to_numpy()
        data_arr = all_df.loc[grouped_df.index, data_col].to_numpy()

        change_arr = ((data_arr[1:] - data_arr[:-1]) / data_arr[:-1])

        all_df.loc[grouped_df.index[1:], col_name_p] =  is_positiv_arr[:-1] * change_arr
        all_df.loc[grouped_df.index[1:], col_name_n] =  is_negativ_arr[:-1] * change_arr

def load_data() -> pd.DataFrame:
    df = pd.read_csv("./data/features_ratios.csv")

    df.period = pd.to_datetime(df.period)
    df.filed = pd.to_datetime(df.filed)

    df.sort_values('period', inplace = True)
    df.reset_index(inplace = True)
    return df

list_data_cols = ['Revenues_hj', 'GrossProfit', 'OperatingIncomeLoss_hj', 'NetIncomeLoss_hj', 'RetainedEarnings_hj', 'Equity_hj']
list_new_cols  = ['gr_revenue' , 'gr_grosspr' , 'gr_opiincome',           'gr_netincome',     'gr_earnings',         'gr_equity']

list_data_cols.extend(['AssetsCurrent', 'AssetsNoncurrent', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent'])
list_new_cols.extend( ['gr_asscur',     'gr_assnoncur',     'gr_liabcur',         'gr_liabnoncur'        ])

list_data_cols.extend(['CashFromInvesting', 'CashFromFinancing', 'CashFromOperating', 'PaymentsOfDividendsTotal_hj'])
list_new_cols.extend( ['gr_cashfrominv',    'gr_cashfromfin',    'gr_cashfromope',    'gr_dividends'])

df = load_data()

def call_function(x):
    print(x[0])
    calculate_growth(df, x[1], list_data_cols, list_new_cols)


if __name__ ==  '__main__':
    import time
    import numpy as np
    np.seterr(all='ignore')

    start = time.time()
    pool = Pool(7)
    pool.map(call_function, df.groupby(['cik', 'fp']))
    pool.close()
    pool.join()
    print("duration: ", start - time.time())