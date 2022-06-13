'''
Created on 2020年1月30日

@author: JM
'''
from datetime import datetime
from datetime import timedelta
# import mysqlclient
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

engine_ts = create_engine('mysql://root:Azsxdcfv0@127.0.0.1:3306/stock?charset=utf8&use_unicode=1')
pro = ""


def read_data():
    sql = """SELECT * FROM stock_basic LIMIT 20"""
    return pd.read_sql_query(sql, engine_ts)


def write_data(df):
    res = df.to_sql('stock_basic', engine_ts, index=False, if_exists='append', chunksize=5000)
    print(res)


def get_basic_inf():

    with engine_ts.connect() as con:
        con.execute("""truncate table {} ;""".format("stock_basic"))
        con.execute("""truncate table {} ;""".format("hs_const"))
        con.execute("""truncate table {} ;""".format("stock_company"))
        con.execute("""truncate table {} ;""".format("bak_basic"))

    # 查询当前所有正常上市交易的股票列表
    pro.stock_basic().to_sql('stock_basic', engine_ts, index=False, if_exists='append', chunksize=5000)
    # 获取沪股通成分
    pro.hs_const(hs_type='SH').to_sql('hs_const', engine_ts, index=False, if_exists='append', chunksize=5000)

    # 获取深股通成分
    pro.hs_const(hs_type='SZ').to_sql('hs_const', engine_ts, index=False, if_exists='append', chunksize=5000)

    # 上市公司基本信息
    pro.stock_company().to_sql('stock_company', engine_ts, index=False, if_exists='append', chunksize=5000)

    # 备用列表
    pro.bak_basic().to_sql('bak_basic', engine_ts, index=False, if_exists='append', chunksize=5000)


def get_last_date():
    sql = """SELECT trade_date FROM last_date where ts_code='all' """
    return pd.read_sql_query(sql, engine_ts)


def set_last_date():
    sql = """SELECT trade_date FROM `daily` order by trade_date desc limit 1 """
    last_day = pd.read_sql_query(sql, engine_ts)['trade_date'][0]
    sql = """ update last_date set trade_date ='{}' where ts_code='all';""".format(last_day)
    with engine_ts.connect() as con:
        con.execute(sql)
    print("Daily 日期更新为{}".format(last_day))


def refresh(start_day):
    # 日期转换为字符串
    sd = start_day.strftime("%Y%m%d")
    # 获取数据
    df = pro.daily(start_date=sd)
    # 如果有新数据，写入数据库
    if len(df) > 0:
        df.to_sql('daily', engine_ts, index=False, if_exists='append', chunksize=5000)
        print("新数据已入库生成！")
        # 更新数据日期
        set_last_date()
    else:
        print("新数据尚未生成！")


if __name__ == '__main__':
    # 获取pro接口
    pro = ts.pro_api()
    choose = input("是否更新基础信息 Y/N?").upper()
    if choose == 'Y':
        get_basic_inf()

    # 从数据库获得上次更新日期
    last_date = get_last_date()
    # 取得'trade_date'列，第0行
    last_date = datetime.strptime(last_date['trade_date'][0], "%Y%m%d").date()
    # 取得当前日期
    today = datetime.now().date()
    # 更新日常数据，条件：
    # 1.日期差大于1
    # 2.日期差大于0，当前时间已过16点
    if (today - last_date).days > 1 or \
            ((today - last_date).days > 0 and datetime.now().time() >
             datetime.strptime("150000", "%H%M%S").time()):
        # 获取更新日期加1天之后的数据
        refresh(last_date + timedelta(days=1))

    # get_basic_inf()
    # get_daily()

    # df = get_data()
    # write_data(df)
    # print(df)
