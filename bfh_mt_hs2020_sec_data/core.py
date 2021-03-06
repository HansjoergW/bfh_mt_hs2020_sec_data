# AUTOGENERATED! DO NOT EDIT! File to edit: 00_core.ipynb (unless otherwise specified).

__all__ = ['get_spark_session', 'get_directory_size', 'get_size_format', 'load_data', 'spark_shape',
           'complete_addition', 'copy_if_not_empty', 'sum_into_empty_target', 'sum_cols_into_new_target',
           'copy_if_not_empty_for_ticker', 'set_to_zero_if_null', 'print_null_count']

# Cell
import findspark
from pyspark.sql import SparkSession
def get_spark_session(appname = "default"):
    """
    Initialises a spark session.
    Parameters:
    appname - default is "default"
    """
    findspark.init()
    return SparkSession.builder \
                        .appName(appname) \
                        .getOrCreate()

# Cell
# code copied from https://www.thepythoncode.com/article/get-directory-size-in-bytes-using-python
import os
def get_directory_size(directory):
    """
    Returns the `directory` size in bytes.
    code copied from https://www.thepythoncode.com/article/get-directory-size-in-bytes-using-python
    """
    total = 0
    try:
        # print("[+] Getting the size of", directory)
        for entry in os.scandir(directory):
            if entry.is_file():
                # if it's a file, use stat() function
                total += entry.stat().st_size
            elif entry.is_dir():
                # if it's a directory, recursively call this function
                total += get_directory_size(entry.path)
    except NotADirectoryError:
        # if `directory` isn't a directory, get the file size then
        return os.path.getsize(directory)
    except PermissionError:
        # if for whatever reason we can't open the folder, return 0
        return 0
    return total

# Cell
# code copied from https://www.thepythoncode.com/article/get-directory-size-in-bytes-using-python
def get_size_format(b, factor=1024, suffix="B"):
    """
    Scale bytes to its proper byte format
    e.g:
        1253656 => '1.20MB'
        1253656678 => '1.17GB'
    code copied from https://www.thepythoncode.com/article/get-directory-size-in-bytes-using-python
    """
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if b < factor:
            return f"{b:.2f}{unit}{suffix}"
        b /= factor
    return f"{b:.2f}Y{suffix}"

# Cell
def load_data(folder, spark, stmt:str, attr:str):
    """ Loads the pivoted data into a spark dataframe.
    """
    return spark.read.parquet(folder + "/" + stmt + "/" + attr)

# Cell
def spark_shape(self):
    return (self.count(), len(self.columns))

# Cell
def complete_addition(df, sumcol, addcol1, addcol2):
    """
    If there are columns that share the relation sumcol = addcol1 + addcol2
    this function ensures that a missing value is calculated based on the other two
    """
    missingtwo = (df[sumcol].notnull()) & (df[addcol1].notnull()) & (df[addcol2].isnull())
    df.loc[missingtwo, addcol2] = df.loc[missingtwo, sumcol] - df.loc[missingtwo, addcol1]

    missingone = (df[sumcol].notnull()) & (df[addcol2].notnull()) & (df[addcol1].isnull())
    df.loc[missingone, addcol1] = df.loc[missingone, sumcol] - df.loc[missingone, addcol2]

    missingsum = (df[sumcol].isnull()) & (df[addcol2].notnull()) & (df[addcol1].notnull())
    df.loc[missingsum, sumcol] = df.loc[missingsum, addcol1] + df.loc[missingsum, addcol2]

# Cell
def copy_if_not_empty(df, sourcecol, targetcol, to_zero_col = None):
    """
    copies the value from the sourceol to the targetcol if the sourcecol is not empty and the targetcol is empty.
    As a third parameter, a column can be provided that has to be set to 0.0 in the rows where the values are copied
    """
    do_copy = (df[sourcecol].notnull()) & (df[targetcol].isnull())
    df.loc[do_copy, targetcol] = df.loc[do_copy, sourcecol]
    if to_zero_col != None:
        df.loc[do_copy, to_zero_col] = 0.0

# Cell
def sum_into_empty_target(df, add1col, add2col, targetcol):
    """
    adds the value of two not empty columns and stores the result in the empty targetcol
    """
    do_sum = (df[add1col].notnull()) & (df[add2col].notnull()) & (df[targetcol].isnull())
    df.loc[do_sum, targetcol] = df.loc[do_sum, add1col] + df.loc[do_sum, add2col]

# Cell
def sum_cols_into_new_target(df, targetcol, sumcolslist):
    """ sums up the value  of several columns and stores the result in the targetcol.
        the columns that contain values to sum up may not be empty.
    """
    df[targetcol] = 0.0
    for col in sumcolslist:
        set_to_zero_if_null(df, col)
        df[targetcol] += df[col]
    return df

# Cell
def copy_if_not_empty_for_ticker(df, ticker, sourcecol, targetcol, to_zero_col = None):
    """ copy the not empty sourcecol to the empty targetcol of a certain ticker.
        if provided, set the to_zero_col for the affected rows to zero.
    """
    do_copy = (df['ticker'] == ticker) & (sel_df[sourcecol].notnull()) & (sel_df[targetcol].isnull())

    df.loc[do_copy, targetcol] = df.loc[do_copy, sourcecol]
    if to_zero_col != None:
        df.loc[do_copy, to_zero_col] = 0.0

# Cell
def set_to_zero_if_null(df, col):
    """ set null values of a column to 0.0
    """
    do_set = (df[col].isnull())
    df.loc[do_set, col] = 0.0

# Cell
def print_null_count(df, cols):
    """ print out howmany null values the provided cols contain
    """
    for col in cols:
        print(col, ' ', df[col].isnull().sum())