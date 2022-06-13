from operator import length_hint

from bson import Int64
import akshare as ak
import pandas as pd
import pandas_ta as ta
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine

file = open("db.txt", "r")
db_con = file.read()
engine_ts = create_engine(db_con)
stock_info_sh_df = pd.DataFrame()
stock_info_sz_df = pd.DataFrame()
stock_info_bj_name_code_df = pd.DataFrame()

def get_basic_inf():
    global stock_info_sh_df, stock_info_sz_df, stock_info_bj_name_code_df
    # 先清表
    with engine_ts.connect() as con:
        con.execute("""truncate table {} ;""".format("stock_info_a_code_name"))
        con.execute("""truncate table {} ;""".format("stock_info_sh_name_code"))
        con.execute("""truncate table {} ;""".format("stock_info_sz_name_code"))
        con.execute("""truncate table {} ;""".format("stock_info_bj_name_code"))
    # 获取全部A股代码
    stock_info_a_code_name_df = ak.stock_info_a_code_name()
    stock_info_a_code_name_df.to_sql("stock_info_a_code_name", engine_ts, index=False, if_exists='append', chunksize=5000)
    #获取上证股票基本信息
    stock_info_sh_df = ak.stock_info_sh_name_code(indicator="主板A股")
    stock_info_sh_df.loc[:, '类型'] = "主板A股"
    tmp = ak.stock_info_sh_name_code(indicator="主板B股")
    tmp.loc[:, '类型'] = "主板B股"
    stock_info_sh_df.append(tmp)
    tmp = ak.stock_info_sh_name_code(indicator="科创板")
    tmp.loc[:, '类型'] = "科创板"
    stock_info_sh_df.append(tmp)
    stock_info_sh_df.to_sql("stock_info_sh_name_code", engine_ts, index=False, if_exists='append', chunksize=5000)
    #获取深证股票基本信息
    stock_info_sz_df = ak.stock_info_sz_name_code(indicator="A股列表")
    stock_info_sz_df["A股总股本"] = stock_info_sz_df["A股总股本"].str.replace(',', '')
    stock_info_sz_df["A股流通股本"] = stock_info_sz_df["A股流通股本"].str.replace(',', '')
    print(stock_info_sz_df)
    stock_info_sz_df["A股总股本"], stock_info_sz_df["A股流通股本"]= \
        pd.to_numeric(stock_info_sz_df["A股总股本"]), pd.to_numeric(stock_info_sz_df["A股流通股本"])
        #  pd.to_numeric(stock_info_sz_df["A股总股本"]),  pd.to_numeric(stock_info_sz_df["A股流通股本"])

    stock_info_sz_df.to_sql("stock_info_sz_name_code", engine_ts, index=False, if_exists='append', chunksize=5000)
    # 获取北证股票基本信息
    stock_info_bj_name_code_df = ak.stock_info_bj_name_code()
    stock_info_bj_name_code_df.to_sql("stock_info_bj_name_code", engine_ts, index=False, if_exists='append', chunksize=5000)


    # stock_list = ak.stock_zh_a_spot_em()
    # stock_zh_a_spot_em_df = stock_list[['代码', '名称', '市盈率-动态', "市净率", "总市值", "流通市值"]]
    # print(stock_zh_a_spot_em_df)





def run_strategy(df):
    # Create your own Custom Strategy
    CustomStrategy = ta.Strategy(
        name="Momo and Volatility",
        description="SMA 50,200, BBANDS, RSI, MACD and Volume SMA 20",
        ta=[
            {"kind": "sma", "length": 50},
            {"kind": "sma", "length": 200},
            {"kind": "macd", "fast": 8, "slow": 21},
            {"kind":"kdj","length":9, "signal":3},
            {"kind": "bbands", "length": 20},
            {"kind": "rsi"},
            {"kind": "sma", "close": "volume", "length": 20, "prefix": "VOLUME"},
        ]
    )
    # To run your "Custom Strategy"
    df.ta.strategy(CustomStrategy)
    return 


def get_df():
    # stock_list = ak.stock_zh_a_spot_em()
    # stock_zh_a_spot_em_df = stock_list[['代码', '名称', '市盈率-动态', "市净率", "总市值", "流通市值"]]
    # print(stock_zh_a_spot_em_df)
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="002286", period="daily", start_date="20170301", end_date='20220610', adjust="qfq")
    
    # VWAP requires the DataFrame index to be a DatetimeIndex.
    # Replace "datetime" with the appropriate column from your DataFrame
    stock_zh_a_hist_df.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'turnover', '振幅', '涨跌幅' , '涨跌额', 'turnover rate']
    stock_zh_a_hist_df.set_index(pd.DatetimeIndex(stock_zh_a_hist_df["datetime"]), inplace=True)
    # print(stock_zh_a_hist_df)
    return stock_zh_a_hist_df
    
def test_func(df):
    # Calculate Returns and append to the df DataFrame
    # be_called_function = getattr(self, name)
    df.ta.AllStrategy(cumulative=True, append=True)
    df.ta.AllStrategy(cumulative=True, append=True)



    # New Columns with results
    print(df.columns)

    # Take a peek
    print(df.tail())

    # vv Continue Post Processing vv


def list_func():
    AllStrategy = ta.AllStrategy
    print("name =", AllStrategy.name)
    print("description =", AllStrategy.description)
    print("created =", AllStrategy.created)
    print("ta =", AllStrategy.ta)


if __name__ == "__main__":
    # choose = input("是否更新基础信息 Y/N?").upper()
    db_con = pd.read_csv('db.txt')

    choose = 'Y'
    if choose == 'Y':
        get_basic_inf()
    df = get_df()
    # print(df)
    run_strategy(df)
    # df.ta.macd(length=10, append=True)
    # print(df)

    # df.ta.strategy(ta.AllStrategy)
    print(df)
    print(df.shape)


