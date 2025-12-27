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
        :param code: 股票代码, 如 "sh.600000"
        """
        # Akshare 接口需要纯数字代码 (600000)
        code_str = self._format_code(code)
        
        try:
            df = ak.stock_financial_abstract_ths(symbol=code_str, indicator="按报告期")
            
            if df is None or df.empty:
                return pd.DataFrame()

            # --- 🛠️ [关键修复] 手动添加 code 列 ---
            # 接口返回的数据里没有 code，必须手动加上，否则 ParquetStorage 无法命名文件
            # 我们使用传入的原始代码 (e.g. sh.600000)，保持与行情文件一致
            df['code'] = code 

            return df
            
        except Exception as e:
            # 某些股票（如新股）可能确实查不到财报，属正常现象，打个日志即可
            # print(f"⚠️ No financial data for {code}: {e}")
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
        """获取同花顺概念板块日线"""
        try:
            df = ak.stock_board_concept_index_ths(
                symbol=concept_name, 
                start_date=start_date, 
                end_date=end_date
            )
            
            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名
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
            # print(f"❌ Error fetching concept history for {concept_name}: {e}")
            return pd.DataFrame()

    # =================================================
    # 3. 🏭 行业与另类数据
    # =================================================
    def fetch_industry_pe_snapshot(self, date: str) -> pd.DataFrame:
        """证监会行业市盈率 (巨潮) date="YYYYMMDD" """
        try:
            return ak.stock_industry_pe_ratio_cninfo(symbol="证监会行业分类", date=date)
        except Exception:
            return pd.DataFrame()

    def fetch_cctv_news(self, date: str) -> pd.DataFrame:
        """新闻联播 date="YYYYMMDD" """
        try:
            return ak.news_cctv(date=date)
        except Exception:
            return pd.DataFrame()

    def fetch_market_pe(self) -> pd.DataFrame:
        """获取A股主板市盈率 (乐咕乐股)"""
        try:
            return ak.stock_market_pe_lg(symbol="上证")
        except Exception as e:
            print(f"❌ Error fetching market PE: {e}")
            return pd.DataFrame()

    def fetch_market_pb(self) -> pd.DataFrame:
        """获取A股等权重/中位数市净率"""
        try:
            return ak.stock_a_all_pb()
        except Exception as e:
            print(f"❌ Error fetching market PB: {e}")
            return pd.DataFrame()

    def _format_code(self, code: str) -> str:
        """去除前缀 sh.600000 -> 600000"""
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