# -*- coding: utf-8 -*-
"""
Baostock 数据抽取接口
对应原脚本: data_downloader.py
功能: 封装 Baostock 官方 API，提供个股日线、指数、成分股等数据的获取
特点: 
    - 自动处理交易日回溯 (依赖 src.utils.date_utils)
    - 自动进行数值类型转换
    - 支持 Context Manager (with 语句)
"""

import baostock as bs
import pandas as pd
import datetime
import sys
from pathlib import Path
from typing import List, Optional

# ==========================================
# 🚑 路径补丁：解决直接运行脚本时的 ModuleNotFoundError
# ==========================================
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

# 导入日期工具 (必须在路径补丁之后)
from src.utils.date_utils import get_latest_trading_date

class BaostockFetcher:
    # 定义默认请求的字段 (日频)
    DAILY_FIELDS = (
        "date,code,open,high,low,close,preclose,volume,amount,"
        "adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
    )
    
    # 定义默认请求的字段 (指数/5分钟)
    INDEX_FIELDS = "date,code,open,high,low,close,preclose,volume,amount,adjustflag"

    def __init__(self):
        self._is_login = False

    def __enter__(self):
        """支持 with 语句：进入时自动登录"""
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持 with 语句：退出时自动登出"""
        self.logout()

    def login(self):
        if not self._is_login:
            lg = bs.login()
            if lg.error_code == '0':
                self._is_login = True
                # print(f"✅ Baostock login success: {lg.error_msg}") 
            else:
                print(f"❌ Baostock login failed: {lg.error_msg}")

    def logout(self):
        if self._is_login:
            bs.logout()
            self._is_login = False
            # print("👋 Baostock logout success")

    def fetch_all_stock_codes(self, date: str = None) -> List[str]:
        """
        获取指定日期的全市场股票列表
        :param date: "YYYY-MM-DD", 默认为最近一个交易日 (自动回溯)
        :return: ['sh.600000', 'sz.000001', ...]
        """
        if date is None:
            # === 使用工具函数获取最近交易日 ===
            # 解决周末运行 Baostock 返回空列表的问题
            date = get_latest_trading_date()
            # print(f"📅 Fetching stock list for date: {date}")

        rs = bs.query_all_stock(day=date)
        stock_list = []
        
        if rs.error_code != '0':
            print(f"Error querying stock list: {rs.error_msg}")
            return []

        while rs.next():
            # row format: [code, tradeStatus, code_name]
            stock_list.append(rs.get_row_data()[0])
            
        return stock_list

    def fetch_daily_kline(self, 
                          code: str, 
                          start_date: str, 
                          end_date: str, 
                          adjust: str = "3") -> pd.DataFrame:
        """
        获取个股日频K线数据
        :param code: 股票代码 e.g. "sh.600000"
        :param start_date: "YYYY-MM-DD"
        :param end_date: "YYYY-MM-DD"
        :param adjust: 复权标识 "3":不复权, "1":后复权, "2":前复权
        :return: DataFrame
        """
        rs = bs.query_history_k_data_plus(
            code,
            self.DAILY_FIELDS,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=adjust
        )

        return self._process_result(rs)

    def fetch_index_kline(self, 
                          code: str, 
                          start_date: str, 
                          end_date: str) -> pd.DataFrame:
        """
        获取指数日频K线数据 (如 sh.000001 上证指数)
        """
        rs = bs.query_history_k_data_plus(
            code,
            self.INDEX_FIELDS,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3" # 指数通常不复权
        )
        
        return self._process_result(rs)

    def fetch_hs300_components(self, date: str = None) -> pd.DataFrame:
        """获取沪深300成分股"""
        if date is None:
            date = get_latest_trading_date()
            
        rs = bs.query_hs300_stocks(date=date)
        return self._process_result(rs)
        
    def fetch_stock_industry(self, code: str) -> pd.DataFrame:
        """获取个股行业分类"""
        rs = bs.query_stock_industry(code=code)
        return self._process_result(rs)

    def _process_result(self, rs) -> pd.DataFrame:
        """内部工具：将 Baostock 的 result 对象转为 DataFrame 并处理类型"""
        if rs.error_code != '0':
            # print(f"Baostock Query Error: {rs.error_msg}")
            return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)
        df = self._convert_types(df)
        return df

    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """内部工具：将字符串数值转换为 float/int"""
        float_cols = [
            'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'turn', 'pctChg', 
            'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM'
        ]
        
        for col in df.columns:
            if col in float_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        if 'isST' in df.columns:
             df['isST'] = pd.to_numeric(df['isST'], errors='coerce').fillna(0).astype(int)

        return df

# --- 简单测试代码 ---
if __name__ == "__main__":
    with BaostockFetcher() as fetcher:
        print("1. 测试获取股票列表 (自动回溯交易日):")
        stocks = fetcher.fetch_all_stock_codes()
        print(f"获取数量: {len(stocks)}")
        print(stocks[:5])
        
        if stocks:
            test_code = stocks[0]
            print(f"\n2. 测试获取日线 ({test_code}):")
            df = fetcher.fetch_daily_kline(test_code, "2023-01-01", "2023-01-10", adjust='1')
            print(df.head())