# -*- coding: utf-8 -*-
"""
Akshare 数据抽取接口
对应原脚本: finance_data_fetcher.py, concept_data_fetcher.py
功能: 封装 Akshare 接口，严格使用同花顺(THS)源获取财务、概念数据
"""

import akshare as ak
import pandas as pd
import sys
from pathlib import Path

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
        获取个股财务报表数据 (同花顺-按报告期)
        
        该接口返回的数据包含了你所需的：
        - 基础报表: 净利润, 营业总收入, 每股净资产等
        - 财务比率: 销售净利率, ROE, 流动比率, 速动比率, 资产负债率等
        
        :param code: 股票代码, 如 "600000"
        :return: DataFrame (包含 '报告期', '净利润', ... '资产负债率' 等字段)
        """
        code_str = self._format_code(code)
        try:
            # 你的 finance_data_fetcher.py 中使用的正是此接口
            df = ak.stock_financial_abstract_ths(symbol=code_str, indicator="按报告期")
            
            if df is None or df.empty:
                return pd.DataFrame()

            # Akshare 返回的列名即为中文: "报告期", "净利润", "流动比率" 等
            # 直接返回，由后续 Storage 层或 Cleaner 层处理列名标准化
            return df
            
        except Exception as e:
            print(f"❌ Error fetching financial report for {code}: {e}")
            return pd.DataFrame()

    # =================================================
    # 2. 💡 概念板块数据 (Concept - THS)
    # =================================================
    def fetch_concept_boards(self) -> pd.DataFrame:
        """获取同花顺概念板块列表"""
        try:
            print("正在获取同花顺概念板块列表...")
            return ak.stock_board_concept_name_ths()
        except Exception as e:
            print(f"❌ Error fetching THS concept boards: {e}")
            return pd.DataFrame()

    def fetch_concept_daily(self, concept_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取同花顺概念板块日线
        :param start_date: "YYYYMMDD"
        """
        try:
            df = ak.stock_board_concept_index_ths(
                symbol=concept_name, 
                start_date=start_date, 
                end_date=end_date
            )
            
            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名，方便存入 Parquet
            rename_map = {
                '日期': 'date', '开盘价': 'open', '最高价': 'high',
                '最低价': 'low', '收盘价': 'close', '成交量': 'volume', '成交额': 'amount'
            }
            df = df.rename(columns=rename_map)
            df['date'] = pd.to_datetime(df['date'])
            
            # 转换数值类型
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df

        except Exception as e:
            print(f"❌ Error fetching concept history for {concept_name}: {e}")
            return pd.DataFrame()

    # =================================================
    # 3. 🏭 行业与另类数据
    # =================================================
    def fetch_industry_pe_snapshot(self, date: str) -> pd.DataFrame:
        """证监会行业市盈率 (巨潮) date="YYYYMMDD" """
        try:
            return ak.stock_industry_pe_ratio_cninfo(symbol="证监会行业分类", date=date)
        except Exception as e:
            return pd.DataFrame()

    def fetch_cctv_news(self, date: str) -> pd.DataFrame:
        """新闻联播 date="YYYYMMDD" """
        try:
            return ak.news_cctv(date=date)
        except Exception as e:
            print(f"❌ Error fetching CCTV news: {e}")
            return pd.DataFrame()

    def fetch_stock_valuation(self, code: str) -> pd.DataFrame:
        """个股总市值 (百度股市通)"""
        code_str = self._format_code(code)
        try:
            return ak.stock_zh_valuation_baidu(symbol=code_str, indicator="总市值", period="全部")
        except Exception as e:
            print(f"❌ Error fetching valuation for {code}: {e}")
            return pd.DataFrame()

    def _format_code(self, code: str) -> str:
        """去除前缀 sh.600000 -> 600000"""
        if isinstance(code, str) and (code.startswith("sh.") or code.startswith("sz.")):
            return code.split(".")[1]
        return str(code)
    
    # =================================================
    # 4. 📊 全市场估值数据 (Market Metrics)
    # =================================================
    def fetch_market_pe(self) -> pd.DataFrame:
        """获取A股主板市盈率 (乐咕乐股) - 返回历史序列"""
        try:
            return ak.stock_market_pe_lg(symbol="上证")
        except Exception as e:
            print(f"❌ Error fetching market PE: {e}")
            return pd.DataFrame()

    def fetch_market_pb(self) -> pd.DataFrame:
        """获取A股等权重/中位数市净率 - 返回历史序列"""
        try:
            return ak.stock_a_all_pb()
        except Exception as e:
            print(f"❌ Error fetching market PB: {e}")
            return pd.DataFrame()

# --- 测试逻辑 ---
if __name__ == "__main__":
    fetcher = AkshareFetcher()
    
    # 测试: 上面所有的接口
    print("1. 测试获取个股财务报表:")
    fin_df = fetcher.fetch_financial_report("600000")
    print(fin_df.head())
    print("\n2. 测试获取概念板块列表:")
    concept_df = fetcher.fetch_concept_boards()
    print(concept_df.head())
    print("\n3. 测试获取概念板块日线:")
    concept_daily_df = fetcher.fetch_concept_daily("人工智能", "20230101", "20231231")
    print(concept_daily_df.head())
    print("\n4. 测试获取行业市盈率快照:")
    industry_pe_df = fetcher.fetch_industry_pe_snapshot(date="20251226")
    print(industry_pe_df.head())
    print("\n5. 测试获取新闻联播:")
    cctv_news_df = fetcher.fetch_cctv_news("20231231")
    print(cctv_news_df.head())
    print("\n6. 测试获取个股总市值:")
    valuation_df = fetcher.fetch_stock_valuation("600000")
    print(valuation_df.head())
