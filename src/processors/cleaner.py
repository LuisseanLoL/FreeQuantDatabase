# -*- coding: utf-8 -*-
"""
数据清洗与标准化处理器
路径: src/processors/cleaner.py
功能: 将不同数据源(Baostock, Akshare, Mootdx)的原始DataFrame清洗为统一格式
"""

import pandas as pd
import numpy as np
import re
import sys
from pathlib import Path

# 🚑 路径补丁
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

class DataCleaner:
    
    # 财务字段中英映射字典 (根据 Akshare 返回表头定制)
    FINANCIAL_MAP = {
        # 基础字段
        "报告期": "report_date",
        "code": "code",
        
        # 利润表
        "净利润": "net_profit",
        "净利润同比增长率": "net_profit_yoy",
        "扣非净利润": "net_profit_dedt",
        "扣非净利润同比增长率": "net_profit_dedt_yoy",
        "营业总收入": "total_revenue",
        "营业总收入同比增长率": "revenue_yoy",
        
        # 每股指标
        "基本每股收益": "eps",
        "每股净资产": "bps",
        "每股资本公积金": "capital_reserve_ps",
        "每股未分配利润": "undistributed_profit_ps",
        "每股经营现金流": "operating_cash_flow_ps",
        
        # 财务比率
        "销售净利率": "net_profit_margin",
        "销售毛利率": "gross_profit_margin",
        "净资产收益率": "roe",
        "净资产收益 率": "roe", # 容错处理
        "净资产收益率-摊薄": "roe_diluted",
        "流动比率": "current_ratio",
        "速动比率": "quick_ratio",
        "保守速动比率": "conservative_quick_ratio",
        "产权比率": "equity_ratio",
        "资产负债率": "debt_to_assets_ratio",
        
        # 营运能力
        "营业周期": "operating_cycle",
        "存货周转率": "inventory_turnover",
        "存货周转天数": "inventory_turnover_days",
        "应收账款周转天数": "receivables_turnover_days"
    }

    @staticmethod
    def normalize_date(df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
        """统一日期格式为 datetime.date (YYYY-MM-DD)"""
        if df.empty or date_col not in df.columns:
            return df
            
        try:
            # 自动处理 YYYY-MM-DD, YYYYMMDD 等格式
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
            # 删除转换失败的脏数据
            df = df.dropna(subset=[date_col])
        except Exception as e:
            print(f"⚠️ Date normalization failed: {e}")
            
        return df

    @staticmethod
    def clean_daily_market_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗日频行情数据 (适用于 Baostock, Mootdx, Concept, Sina)
        - 确保日期格式统一
        - 确保 OHLCV 为数值型
        - 处理 Baostock 的 'adjustflag' 等
        """
        if df.empty:
            return df
            
        # 1. 统一列名小写 (防坑)
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        # 2. 日期处理
        df = DataCleaner.normalize_date(df, 'date')
        
        # 3. 数值强转 (Baostock有时返回字符串)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctchg', 'pettm']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        # 4. 特殊字段处理
        if 'isst' in df.columns:
             # 将 '1'/'0' 字符串转为 int
             df['isst'] = pd.to_numeric(df['isst'], errors='coerce').fillna(0).astype(int)

        return df

    @staticmethod
    def clean_financial_report(df: pd.DataFrame) -> pd.DataFrame:
        """
        [核心] 清洗 Akshare 财务报表
        - 中文列名映射为英文
        - 清洗 '6.29亿', '2.53%', 'False', '--' 等特殊格式
        """
        if df.empty:
            return df

        # 1. 字段映射 (中文 -> 英文)
        # 仅保留我们在 map 中定义的列，或者保留全部并重命名
        df = df.rename(columns=DataCleaner.FINANCIAL_MAP)
        
        # 2. 日期标准化
        if 'report_date' in df.columns:
            df = DataCleaner.normalize_date(df, 'report_date')
            
        # 3. 这里的列现在大部分是英文了，我们需要清洗数值
        # 需要清洗的列：所有除了 date 和 code 之外的列
        exclude_cols = ['code', 'report_date', 'date']
        target_cols = [c for c in df.columns if c not in exclude_cols]

        def clean_value(val):
            """内部函数：清洗单个单元格"""
            if pd.isna(val): return np.nan
            
            s = str(val).strip()
            
            # 处理无效字符串
            if s in ['False', 'None', '--', '', 'nan']:
                return np.nan
            
            # 处理百分比 '2.53%' -> 0.0253
            if '%' in s:
                try:
                    return float(s.replace('%', '')) / 100.0
                except:
                    return np.nan
            
            # 处理单位 '6.29亿' -> 6.29 * 10^8
            if '亿' in s:
                try:
                    return float(s.replace('亿', '')) * 1e8
                except:
                    return np.nan
            if '万' in s:
                try:
                    return float(s.replace('万', '')) * 1e4
                except:
                    return np.nan
            
            # 尝试直接转数字
            try:
                return float(s)
            except:
                return np.nan

        # 4. 批量应用清洗逻辑
        # 注意：使用 applymap 可能较慢，但对于含有混合类型的财务数据是最稳妥的
        # 优化：只对 object 类型的列处理
        for col in target_cols:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(clean_value)
            # 已经是数字类型的列无需处理
            
        return df

    @staticmethod
    def clean_news_data(df: pd.DataFrame) -> pd.DataFrame:
        """清洗新闻数据 (日期格式化)"""
        if df.empty: return df
        
        # Akshare 新闻日期通常是 '20231231' 字符串
        if 'date' in df.columns:
            # 转换为 datetime
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce').dt.date
            
        return df

# ==========================================
# 测试代码 (使用你提供的真实 Log 数据)
# ==========================================
if __name__ == "__main__":
    cleaner = DataCleaner()
    
    print("=== 1. 测试财务数据清洗 ===")
    # 模拟你 Log 中的 Akshare 原始数据
    fin_data = {
        "报告期": ["1996-12-31", "1997-12-31", "1999-12-31"],
        "净利润": ["6.29亿", "6.45亿", "7.29亿"],
        "净利润同比增长率": ["False", "2.53%", "-6.59%"],
        "营业总收入": ["42.16亿", "53.28亿", "49.46亿"],
        "资产负债率": ["97.57%", "96.12%", "92.06%"],
        "code": ["000001", "000001", "000001"]
    }
    df_fin = pd.DataFrame(fin_data)
    print("【原始数据】")
    print(df_fin)
    
    df_fin_clean = cleaner.clean_financial_report(df_fin)
    print("\n【清洗后数据】")
    print(df_fin_clean)
    print("\n【类型检查】")
    print(df_fin_clean.dtypes)
    
    print("\n=== 2. 测试新闻数据清洗 ===")
    news_data = {
        "date": ["20231231", "20240101"],
        "title": ["Title1", "Title2"]
    }
    df_news = cleaner.clean_news_data(pd.DataFrame(news_data))
    print(df_news)