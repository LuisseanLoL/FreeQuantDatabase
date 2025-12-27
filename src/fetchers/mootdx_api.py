# -*- coding: utf-8 -*-
"""
Mootdx 数据抽取接口
对应原脚本: data_importer.ipynb
功能: 封装 Mootdx 接口，获取 ETF 日频量价数据 (含复权)
"""

import pandas as pd
import datetime
from mootdx.quotes import Quotes
from mootdx.contrib.adjust import get_adjust_year
from typing import List, Tuple, Dict, Any

# 🚑 路径补丁
import sys
from pathlib import Path
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)


class MootdxFetcher:
    def __init__(self):
        try:
            # 修改点 1: 关闭心跳 (heartbeat=False)
            # 这里的 client 主要用于测试连通性，或者未来扩展功能
            # get_adjust_year 内部其实会独立创建连接，不依赖这个 client
            self._client = Quotes.factory(market='std', multithread=True, heartbeat=False)
            print("✅ Mootdx client initialized.")
        except Exception as e:
            print(f"❌ Failed to initialize Mootdx client: {e}")
            self._client = None 

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 修改点 2: 显式关闭连接，防止终端卡死
        if self._client:
            try:
                self._client.quit() # type: ignore
                print("👋 Mootdx client closed.")
            except Exception:
                pass

    def fetch_etf_daily_kline(self, 
                              code: str, 
                              ipo_year: int, 
                              start_date: str, 
                              end_date: str,
                              adjust_factor: str = '02') -> pd.DataFrame:
        """
        获取 ETF 复权日线
        :param adjust_factor: '02' (后复权, 默认)
        """
        # 简单检查连接对象是否存在（虽然 get_adjust_year 不直接用它，但代表环境正常）
        if self._client is None:
            return pd.DataFrame()

        start_year_int = datetime.datetime.strptime(start_date, '%Y-%m-%d').year
        end_year_int = datetime.datetime.strptime(end_date, '%Y-%m-%d').year

        all_dfs = []

        # 遍历年份
        for year_int in range(start_year_int, end_year_int + 1):
            if year_int < ipo_year:
                continue

            year_str = str(year_int)
            try:
                # 获取复权数据
                # 注意：mootdx 这里通常返回以日期为 Index 的 DataFrame
                df_year = get_adjust_year(symbol=code, year=year_str, factor=adjust_factor)
                
                if df_year is not None and not df_year.empty:
                    all_dfs.append(df_year)
            except Exception:
                pass 

        if not all_dfs:
            return pd.DataFrame()

        # 合并
        full_df = pd.concat(all_dfs)

        # 🛠️ 关键修复：重置索引，将日期从 Index 变为 Column
        full_df.reset_index(inplace=True)

        # 标准化列名
        full_df.columns = [str(c).lower() for c in full_df.columns]

        rename_map = {
            'index': 'date',    # reset_index 产生的默认名
            'datetime': 'date', # 某些版本的原始名
            'vol': 'volume'     # 标准化成交量
        }
        full_df = full_df.rename(columns=rename_map)

        # 检查 'date' 列
        if 'date' not in full_df.columns:
            return pd.DataFrame()

        # 类型转换与排序
        full_df['date'] = pd.to_datetime(full_df['date']).dt.date
        full_df = full_df.sort_values(by='date').reset_index(drop=True)
        
        # 添加复权因子标识列
        full_df['adjust'] = adjust_factor 

        # 时间范围过滤
        start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        
        full_df_filtered = full_df[
            (full_df['date'] >= start_date_obj) &
            (full_df['date'] <= end_date_obj)
        ].copy()

        # 输出列过滤
        output_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjust']
        final_cols = [col for col in output_cols if col in full_df_filtered.columns]
        
        return full_df_filtered[final_cols]

# --- 测试代码 ---
if __name__ == "__main__":
    etf_pool_test = {
        "HS300": ("510300", 2012),
        "GOLD":  ("518880", 2013)
    }
    # 缩小测试范围，加快速度
    start_date_test = "2023-01-01"
    end_date_test = "2025-12-31" 

    # 使用 with 语句，确保退出时自动调用 __exit__ 进行清理
    with MootdxFetcher() as fetcher:
        print(f"开始测试 Mootdx 数据抓取 ({start_date_test} to {end_date_test})\n")

        for name, (code, ipo_year) in etf_pool_test.items():
            print(f"--- 正在获取 {name} ({code}) ---")
            df_etf = fetcher.fetch_etf_daily_kline(code, ipo_year, start_date_test, end_date_test)

            if not df_etf.empty:
                print(f"✅ 成功获取 {name} 数据，共 {len(df_etf)} 条记录。")
                print(df_etf.head(2))
            else:
                print(f"❌ 未获取到 {name} 的数据。")
            print("-" * 30)
    
    print("程序正常结束。")