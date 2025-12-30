# -*- coding: utf-8 -*-
"""
Akshare 数据抽取接口 (修正版)
功能: 封装 Akshare 接口，严格使用同花顺(THS)源获取财务、概念数据
修正点: 在 fetch_financial_report 中手动注入 'code' 列，解决存储时 missing key error
"""

import akshare as ak
import pandas as pd
import sys
from pathlib import Path
import time # 引入time
from json import JSONDecodeError # 引入具体的错误类型

# 🚑 路径补丁
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

class AkshareFetcher:
    def __init__(self):
        pass

    # =================================================
    # 1. 💰 个股财务数据 (Financial)
    # =================================================
    def fetch_financial_report(self, code: str) -> pd.DataFrame:
        """
        获取个股财务摘要 (全量数据)
        增加: 重试机制与详细的错误捕获
        """
        code_str = self._format_code(code)
        
        # 简单的重试机制
        max_retries = 3
        for i in range(max_retries):
            try:
                df = ak.stock_financial_abstract(symbol=code_str)
                
                if df is None or df.empty:
                    return pd.DataFrame()

                # 手动注入 code
                df['code'] = code 
                return df
            
            except JSONDecodeError:
                # 这是最关键的捕获：说明被反爬了
                print(f"⚠️ [Anti-Scraping] JSON Error for {code}. Retrying ({i+1}/{max_retries})...")
                time.sleep(600) # 遇到封锁，多睡一会
                continue
                
            except Exception as e:
                # 其他网络错误
                # print(f"⚠️ Error fetching {code}: {e}")
                return pd.DataFrame()
        
        # 重试多次后依然失败
        print(f"❌ Failed to fetch {code} after retries.")
        return pd.DataFrame()

    # =================================================
    # 2. 💡 概念板块数据
    # =================================================
    def fetch_concept_boards(self) -> pd.DataFrame:
        try:
            print("正在获取同花顺概念板块列表...")
            return ak.stock_board_concept_name_ths()
        except Exception as e:
            print(f"❌ Error fetching THS concept boards: {e}")
            return pd.DataFrame()

    def fetch_concept_daily(self, concept_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            df = ak.stock_board_concept_index_ths(
                symbol=concept_name, 
                start_date=start_date, 
                end_date=end_date
            )
            if df is None or df.empty: return pd.DataFrame()

            rename_map = {'日期': 'date', '开盘价': 'open', '最高价': 'high','最低价': 'low', '收盘价': 'close', '成交量': 'volume', '成交额': 'amount'}
            df = df.rename(columns=rename_map)
            df['date'] = pd.to_datetime(df['date'])
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        except Exception: return pd.DataFrame()

    # =================================================
    # 3. 另类数据
    # =================================================
    def fetch_industry_pe_snapshot(self, date: str) -> pd.DataFrame:
        try: return ak.stock_industry_pe_ratio_cninfo(symbol="证监会行业分类", date=date)
        except: return pd.DataFrame()

    def fetch_cctv_news(self, date: str) -> pd.DataFrame:
        try: return ak.news_cctv(date=date)
        except: return pd.DataFrame()

    def fetch_market_pe(self) -> pd.DataFrame:
        try: return ak.stock_market_pe_lg(symbol="上证")
        except: return pd.DataFrame()

    def fetch_market_pb(self) -> pd.DataFrame:
        try: return ak.stock_a_all_pb()
        except: return pd.DataFrame()

    def _format_code(self, code: str) -> str:
        if isinstance(code, str) and (code.startswith("sh.") or code.startswith("sz.") or code.startswith("bj.")):
            return code.split(".")[1]
        return str(code)

# --- 测试逻辑 ---
if __name__ == "__main__":
    fetcher = AkshareFetcher()
    test_code = "sh.600000"
    print(f"1. 测试同花顺财务报表 ({test_code}):")
    df_fin = fetcher.fetch_financial_report(test_code)
    
    if not df_fin.empty:
        print("\n[表头字段预览]:")
        print(df_fin.columns.tolist())
        if 'code' in df_fin.columns:
            print("✅ 'code' 列存在，修复成功。")
            print(f"Code Value: {df_fin['code'].iloc[0]}")
        else:
            print("❌ 'code' 列依然缺失！")
    else:
        print("未获取到数据。")