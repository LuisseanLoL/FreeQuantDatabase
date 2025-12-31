# -*- coding: utf-8 -*-
"""
Baostock 数据抽取接口 (增强版)
修正: 增加 fetch_all_stock_codes 的自动回溯机制，防止因日期问题导致获取股票列表为空
"""

import baostock as bs
import pandas as pd
import datetime
import sys
from pathlib import Path
from typing import List, Optional

# 🚑 路径补丁
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.date_utils import get_latest_trading_date

class BaostockFetcher:
    DAILY_FIELDS = (
        "date,code,open,high,low,close,preclose,volume,amount,"
        "adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
    )
    INDEX_FIELDS = "date,code,open,high,low,close,preclose,volume,amount,adjustflag"

    def __init__(self):
        self._is_login = False

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()

    def login(self):
        if not self._is_login:
            lg = bs.login()
            if lg.error_code == '0':
                self._is_login = True
            else:
                print(f"❌ Baostock login failed: {lg.error_msg}")

    def logout(self):
        if self._is_login:
            bs.logout()
            self._is_login = False

    def fetch_all_stock_codes(self, date: str = None) -> List[str]:
        """
        获取全市场股票列表 (带自动回溯重试)
        """
        if date is None:
            date = get_latest_trading_date()
            
        # 尝试回溯的天数上限
        max_retries = 10 
        current_date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")

        for i in range(max_retries):
            query_date = current_date_obj.strftime("%Y-%m-%d")
            
            rs = bs.query_all_stock(day=query_date)
            
            if rs.error_code != '0':
                # 接口报错，跳过
                pass
            else:
                stock_list = []
                while rs.next():
                    stock_list.append(rs.get_row_data()[0])
                
                # 如果获取到了数据，直接返回
                if stock_list:
                    if i > 0:
                        print(f"⚠️ Initial date {date} empty, fell back to {query_date} (Found {len(stock_list)} stocks)")
                    return stock_list
            
            # 如果没获取到数据，日期减1天，继续尝试
            current_date_obj -= datetime.timedelta(days=1)

        print(f"❌ Failed to fetch stock list after {max_retries} retries (Last attempt: {query_date})")
        return []

    def fetch_daily_kline(self, code: str, start_date: str, end_date: str, adjust: str = "3") -> pd.DataFrame:
        rs = bs.query_history_k_data_plus(
            code, self.DAILY_FIELDS,
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag=adjust
        )
        return self._process_result(rs)

    def fetch_index_kline(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        rs = bs.query_history_k_data_plus(
            code, self.INDEX_FIELDS,
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="3"
        )
        return self._process_result(rs)

    def fetch_hs300_components(self, date: str = None) -> pd.DataFrame:
        if date is None: date = get_latest_trading_date()
        rs = bs.query_hs300_stocks(date=date)
        return self._process_result(rs)
        
    def fetch_stock_industry(self, code: str) -> pd.DataFrame:
        rs = bs.query_stock_industry(code=code)
        return self._process_result(rs)

    def _process_result(self, rs) -> pd.DataFrame:
        if rs.error_code != '0': return pd.DataFrame()
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        if not data_list: return pd.DataFrame()
        df = pd.DataFrame(data_list, columns=rs.fields)
        return self._convert_types(df)

    def fetch_profit_data_history(self, code: str, start_year: int = 2010, end_year: int = None) -> pd.DataFrame:
        """
        获取个股历史盈利能力数据 (含pubDate, totalShare等)
        :param code: e.g. "sh.600000"
        :param start_year: 开始年份
        :param end_year: 结束年份 (默认当年)
        """
        if end_year is None:
            end_year = datetime.datetime.now().year

        data_list = []
        
        # 遍历年份和季度
        # 注意: 这里的循环会导致网络请求较多，update模式下建议缩短年份范围
        for year in range(start_year, end_year + 1):
            for quarter in [1, 2, 3, 4]:
                try:
                    rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
                    if rs.error_code == '0':
                        while rs.next():
                            data_list.append(rs.get_row_data())
                except Exception:
                    continue
        
        if not data_list:
            return pd.DataFrame()

        # Baostock返回的原始字段
        # code, pubDate, statDate, roeAvg, npMargin, gpMargin, netProfit, epsTTM, MBRevenue, totalShare, liqaShare
        # 我们暂时不在这里重命名，留给Cleaner处理
        columns = [
            "code", "pubDate", "statDate", "roeAvg", "npMargin", "gpMargin", 
            "netProfit", "epsTTM", "MBRevenue", "totalShare", "liqaShare"
        ]
        
        # 如果返回列数不对，重新获取fields (通常上面硬编码的列名是固定的)
        if data_list and len(data_list[0]) == len(columns):
            df = pd.DataFrame(data_list, columns=columns)
        else:
            # Fallback (极少发生)
            df = pd.DataFrame(data_list)

        return self._convert_types(df)

    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """重写或扩展类型转换，增加对新字段的处理"""
        # 定义浮点列
        float_cols = [
            'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 
            'turn', 'pctChg', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM',
            # 新增的财务字段
            'roeAvg', 'npMargin', 'gpMargin', 'netProfit', 'epsTTM', 
            'MBRevenue', 'totalShare', 'liqaShare'
        ]
        
        for col in df.columns:
            if col in float_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        if 'isST' in df.columns:
             df['isST'] = pd.to_numeric(df['isST'], errors='coerce').fillna(0).astype(int)
             
        return df

if __name__ == "__main__":
    with BaostockFetcher() as fetcher:
        print("Testing fetch_all_stock_codes...")
        stocks = fetcher.fetch_all_stock_codes()
        print(f"Found: {len(stocks)}")