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
    def clean_baostock_profit(df: pd.DataFrame) -> pd.DataFrame:
        """清洗 Baostock 盈利能力数据"""
        if df.empty: return df
        
        # 1. 重命名列以匹配系统标准
        # statDate -> report_date (对齐 Akshare)
        # pubDate -> publish_date
        rename_map = {
            'statDate': 'report_date',
            'pubDate': 'publish_date',
            'totalShare': 'total_share',
            'liqaShare': 'circulating_share' # 流通股
        }
        df = df.rename(columns=rename_map)
        
        # 2. 清洗日期
        df = DataCleaner.normalize_date(df, 'report_date')
        df = DataCleaner.normalize_date(df, 'publish_date')
        
        # 3. 筛选需要的列 (避免和 Akshare 的 netProfit 冲突，只取补充列)
        # 当然，如果你想保留 Baostock 的指标也可以，这里优先保留补充信息
        keep_cols = ['code', 'report_date', 'publish_date', 'total_share', 'circulating_share']
        
        # 确保列存在
        final_cols = [c for c in keep_cols if c in df.columns]
        return df[final_cols]

    @staticmethod
    def merge_financial_data(df_ak: pd.DataFrame, df_bs: pd.DataFrame) -> pd.DataFrame:
        """
        合并 Akshare (主) 和 Baostock (辅) 的财务数据
        Key: code, report_date
        """
        if df_ak.empty: return df_bs
        if df_bs.empty: return df_ak
        
        # 确保关键列类型一致
        for df in [df_ak, df_bs]:
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(df['report_date']).dt.date
            if 'code' in df.columns:
                df['code'] = df['code'].astype(str)

        # Merge
        # 使用 left join，以 Akshare 为主（因为 Akshare 包含所有历史，而 Baostock 可能只抓了近几年的）
        # on=['code', 'report_date']
        try:
            merged_df = pd.merge(
                df_ak, 
                df_bs, 
                on=['code', 'report_date'], 
                how='left', 
                suffixes=('', '_bs') # 如果有重名列，Baostock的加后缀
            )
            return merged_df
        except Exception as e:
            print(f"⚠️ Merge failed: {e}")
            return df_ak

    @staticmethod
    def clean_news_data(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce').dt.date
        return df
    
    @staticmethod
    def _parse_ths_report_period(text: str):
        """
        内部工具: 解析同花顺的中文报告期
        2022年报 -> 2022-12-31
        2022中报 -> 2022-06-30
        2022一季报 -> 2022-03-31
        2022三季报 -> 2022-09-30
        """
        if not isinstance(text, str): return pd.NaT
        
        # 提取年份 (前4位)
        try:
            year = int(text[:4])
        except:
            return pd.NaT
            
        if "年报" in text:
            return pd.Timestamp(year=year, month=12, day=31).date()
        elif "中报" in text:
            return pd.Timestamp(year=year, month=6, day=30).date()
        elif "一季" in text: # 一季报
            return pd.Timestamp(year=year, month=3, day=31).date()
        elif "三季" in text: # 三季报
            return pd.Timestamp(year=year, month=9, day=30).date()
        else:
            return pd.NaT

    @staticmethod
    def clean_dividend_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗分红数据
        """
        if df.empty: return df
        
        # 1. 映射字段名
        # 报告期 -> report_date (用于对齐)
        # 税前分红率 -> dividend_yield
        # 股利支付率 -> dividend_payout_ratio
        # A股除权除息日 -> ex_dividend_date
        rename_map = {
            "报告期": "report_period_str", # 保留原始列备查
            "税前分红率": "dividend_yield",
            "股利支付率": "dividend_payout_ratio",
            "A股除权除息日": "ex_dividend_date",
            "分红总额": "total_dividend"
        }
        df = df.rename(columns=rename_map)
        
        # 2. 处理报告期 (转为标准 date)
        if "report_period_str" in df.columns:
            df['report_date'] = df['report_period_str'].apply(DataCleaner._parse_ths_report_period)
            # 过滤解析失败的行
            df = df.dropna(subset=['report_date'])
        
        # 3. 处理百分比和单位
        def clean_pct(val):
            """ 1.52% -> 0.0152, -- -> NaN """
            if pd.isna(val): return np.nan
            s = str(val).strip()
            if '%' in s:
                try:
                    return float(s.replace('%', '')) / 100.0
                except: return np.nan
            return np.nan

        def clean_amount(val):
            """ 2.94亿 -> 2.94e8 """
            if pd.isna(val): return np.nan
            s = str(val).strip()
            if '亿' in s:
                try:
                    return float(s.replace('亿', '')) * 1e8
                except: return np.nan
            if '万' in s:
                try:
                    return float(s.replace('万', '')) * 1e4
                except: return np.nan
            return np.nan

        if 'dividend_yield' in df.columns:
            df['dividend_yield'] = df['dividend_yield'].apply(clean_pct)
            
        if 'dividend_payout_ratio' in df.columns:
            df['dividend_payout_ratio'] = df['dividend_payout_ratio'].apply(clean_pct)
            
        if 'total_dividend' in df.columns:
            df['total_dividend'] = df['total_dividend'].apply(clean_amount)
            
        if 'ex_dividend_date' in df.columns:
            df['ex_dividend_date'] = pd.to_datetime(df['ex_dividend_date'], errors='coerce').dt.date

        # 4. 筛选保留字段
        keep_cols = ['code', 'report_date', 'dividend_yield', 'dividend_payout_ratio', 'total_dividend', 'ex_dividend_date']
        return df[[c for c in keep_cols if c in df.columns]]