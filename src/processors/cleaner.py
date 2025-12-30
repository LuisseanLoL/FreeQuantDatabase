# -*- coding: utf-8 -*-
"""
数据清洗与标准化处理器 (适配新财务接口)
功能: 
    1. 清洗日频行情
    2. [重点] 将 stock_financial_abstract 的宽表转置为长表，并标准化
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 🚑 路径补丁
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

class DataCleaner:
    
    @staticmethod
    def normalize_date(df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
        """统一日期格式为 datetime.date (YYYY-MM-DD)"""
        if df.empty or date_col not in df.columns:
            return df
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
            df = df.dropna(subset=[date_col])
        except Exception:
            pass
        return df

    @staticmethod
    def clean_daily_market_data(df: pd.DataFrame) -> pd.DataFrame:
        """清洗日频行情数据"""
        if df.empty: return df
        
        df.columns = [str(c).lower().strip() for c in df.columns]
        df = DataCleaner.normalize_date(df, 'date')
        
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctchg', 'pettm']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'isst' in df.columns:
             df['isst'] = pd.to_numeric(df['isst'], errors='coerce').fillna(0).astype(int)

        return df

    @staticmethod
    def clean_financial_report(df: pd.DataFrame) -> pd.DataFrame:
        """
        [核心] 清洗新的 stock_financial_abstract 数据
        输入格式: 
            选项, 指标, 20250930, 20250630, ..., code
            常用指标, 净利润, 1000, 500, ..., sh.600000
        
        输出格式:
            report_date, code, 净利润, 营业总收入, ... (所有指标作为列)
        """
        if df.empty: return df

        # 1. 提取 Code (我们在 Fetcher 里加到了最后一列)
        # 这种宽表结构里，code 列会重复填充在每一行，我们取第一个即可
        stock_code = None
        if 'code' in df.columns:
            stock_code = df['code'].iloc[0]
            # 删掉 code 列，因为它会干扰转置
            df = df.drop(columns=['code'])

        # 2. 转置逻辑 (Unpivot / Melt)
        # 原始列: [选项, 指标, 20250930, 20250630, ...]
        # 我们需要保留 '指标' 列，扔掉 '选项' 列(因为它对区分指标没用，指标名本身通常唯一)
        # 如果指标名有重复（比如 '每股收益' 出现了两次），我们需要去重或合并
        
        if '选项' in df.columns:
            # 也可以保留选项作为前缀，比如 "常用指标_净利润"，防止重名
            # 这里简单起见，我们优先使用 '指标' 列。
            # 检查是否有重名指标，如果有，可以用 选项+指标 组合
            df['unique_key'] = df['指标'] 
            # 简单的去重策略：如果 duplicate，保留第一个
            df = df.drop_duplicates(subset=['unique_key'])
            df = df.drop(columns=['选项', '指标'])
            df = df.set_index('unique_key')
        else:
            # 防御性编程
            return pd.DataFrame()

        # 3. 转置: 现在行是指标，列是日期 -> 转置后 行是日期，列是指标
        # 此时 df.columns 应该是 ['20250930', '20250630', ...]
        df_T = df.T 
        
        # 4. 整理索引 (变成 report_date 列)
        df_T.index.name = 'report_date'
        df_T = df_T.reset_index()
        
        # 5. 清洗日期
        # 列名里的日期可能是 '20250930' 字符串
        df_T['report_date'] = pd.to_datetime(df_T['report_date'], errors='coerce')
        # 删掉日期转换失败的行 (可能是脏数据列)
        df_T = df_T.dropna(subset=['report_date'])
        df_T['report_date'] = df_T['report_date'].dt.date

        # 6. 补回 Code
        if stock_code:
            df_T['code'] = stock_code

        # 7. 自动清洗所有数值列
        # 现在的列名就是之前的指标名 (净利润, ROE...)
        # 遍历除了 date/code 以外的所有列，尝试转 numeric
        exclude = ['report_date', 'code']
        for col in df_T.columns:
            if col not in exclude:
                # 统一转 numeric，无法转换的变 NaN
                df_T[col] = pd.to_numeric(df_T[col], errors='coerce')
                
                # 可选: 列名标准化 (去掉特殊符号)
                # new_col = col.replace("(", "").replace(")", "").replace("%", "")
                # df_T.rename(columns={col: new_col}, inplace=True)

        return df_T

    @staticmethod
    def clean_news_data(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce').dt.date
        return df