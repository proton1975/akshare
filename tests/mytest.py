from operator import length_hint

from bson import Int64
import akshare as ak
import pandas as pd
import pandas_ta as ta
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine

db_con = open("db.txt", "r").read()

engine_ts = create_engine(db_con)
stock_info_a_code_name_df = pd.DataFrame()
stock_info_sh_df = pd.DataFrame()
stock_info_sz_df = pd.DataFrame()
stock_info_bj_name_code_df = pd.DataFrame()
stock_zh_a_hist_df = pd.DataFrame()

# 读取日线数据的最后日期
def get_last_date():
    sql = """SELECT trade_date FROM last_date where ts_code='all' """
    return pd.read_sql_query(sql, engine_ts)

# 修改日线数据的最后日期
def set_last_date():
    sql = """SELECT datetime FROM `stock_zh_a_hist` order by datetime desc limit 1 """
    last_day = pd.read_sql_query(sql, engine_ts)['datetime'][0]
    sql = """ update last_date set trade_date ='{}' where ts_code='all';""".format(last_day)
    with engine_ts.connect() as con:
        con.execute(sql)
    print("Daily 日期更新为{}".format(last_day))
    

def get_all_stock_zh_a_hist(start_date):
    for i in range(len(stock_info_a_code_name_df)):
        get_stock_zh_a_hist([stock_info_a_code_name_df['code'][i], start_date])
    set_last_date()
    

def get_stock_zh_a_hist(para:list):
    # 增加主键
    # sql="alter table akshare_stock.stock_zh_a_hist add column id int auto_increment primary key;"
    start_date = para[1]
    code = para[0]
    global stock_zh_a_hist_df
    # code = "002286"
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, adjust="qfq")
    stock_zh_a_hist_df.loc[:, '代码'] = code
    stock_zh_a_hist_df.loc[:, '复权'] = "qfq"
    tmp = ak.stock_zh_a_hist(symbol="002286", period="daily", start_date=start_date, adjust="hfq")
    tmp.loc[:, '代码'] =code
    tmp.loc[:, '复权'] = "hfq"
    stock_zh_a_hist_df = stock_zh_a_hist_df.append(tmp)
    stock_zh_a_hist_df.columns = \
        ['datetime', 'open', 'close', 'high', 'low', 'volume', 'turnover', '振幅', '涨跌幅' , '涨跌额', 'turnover rate','代码', '复权']
    stock_zh_a_hist_df.to_sql("stock_zh_a_hist", engine_ts, index=False, if_exists='append', chunksize=5000)
    print("股票代码：{} 的历史日数据已更新,数据量{}！".format(code,len(stock_zh_a_hist_df)))



#从数据库中读取基础数据
def get_basic_inf_by_sql():
    global stock_info_a_code_name_df, stock_info_sh_df, stock_info_sz_df, stock_info_bj_name_code_df
    sql = """ select * from stock_info_a_code_name;"""
    stock_info_a_code_name_df = pd.read_sql_query(sql, engine_ts)
    sql = """ select * from stock_info_sh_name_code;"""
    stock_info_sh_df = pd.read_sql_query(sql, engine_ts)
    sql = """ select * from stock_info_sz_name_code;"""
    stock_info_sz_df = pd.read_sql_query(sql, engine_ts)
    sql = """ select * from stock_info_bj_name_code;"""
    stock_info_bj_name_code_df = pd.read_sql_query(sql, engine_ts)

# 从网络读取基础数据
def get_basic_inf():
    global stock_info_a_code_name_df, stock_info_sh_df, stock_info_sz_df, stock_info_bj_name_code_df
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
    stock_info_sh_df = stock_info_sh_df.append(tmp)
    tmp = ak.stock_info_sh_name_code(indicator="科创板")
    tmp.loc[:, '类型'] = "科创板"
    stock_info_sh_df = stock_info_sh_df.append(tmp)
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
    choose = input("是否更新基础信息 Y/N?").upper()


    # choose = 'N'
    if choose == 'Y':
        get_basic_inf()
    else:
        get_basic_inf_by_sql()
    # 从数据库获得上次更新日期
    last_date = get_last_date()
    # 取得'trade_date'列，第0行
    last_date = datetime.strptime(last_date['trade_date'][0], "%Y-%m-%d").date()
    # 取得当前日期
    today = datetime.now().date()
    # 更新日常数据，条件：
    # 1.日期差大于1
    # 2.日期差大于0，当前时间已过16点
    if (today - last_date).days > 1 or \
            ((today - last_date).days > 0 and datetime.now().time() >
             datetime.strptime("160000", "%H%M%S").time()):
        # 获取更新日期加1天之后的数据
        get_all_stock_zh_a_hist(last_date + timedelta(days=1))
        
        # get_stock_zh_a_hist(['002286',last_date + timedelta(days=1)])
    df = get_df()
    # print(df)
    run_strategy(df)
    # df.ta.macd(length=10, append=True)
    # print(df)

    # df.ta.strategy(ta.AllStrategy)
    print(df)
    print(df.shape)


