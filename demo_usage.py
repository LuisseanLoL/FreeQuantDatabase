# -*- coding: utf-8 -*-
"""
FreeQuantDatabase 调用演示 (列名查看版)
功能: 优先调用 data/optimized 下的高性能数据，并打印各数据源的所有可用字段(Columns)
"""

import duckdb
import pandas as pd
import os

# ==========================================
# 1. 环境配置 (保持不变)
# ==========================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

PATH_PROCESSED = os.path.join(CURRENT_DIR, "data", "processed")
PATH_OPTIMIZED = os.path.join(CURRENT_DIR, "data", "optimized")

pd.set_option('display.max_columns', None) # 显示所有列
pd.set_option('display.width', 1000)

print(f"📂 标准数据源: {PATH_PROCESSED}")
print(f"🚀 极速数据源: {PATH_OPTIMIZED}\n")

con = duckdb.connect()

# ==========================================
# 2. 智能注册视图 (Auto-Routing) (保持不变)
# ==========================================

def register_smart_view(view_name, folder_name):
    opt_path = os.path.join(PATH_OPTIMIZED, folder_name)
    proc_path = os.path.join(PATH_PROCESSED, folder_name)
    
    if os.path.exists(opt_path) and len(os.listdir(opt_path)) > 0:
        try:
            sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{opt_path}/*.parquet', union_by_name=true)"
            con.execute(sql)
            print(f"  🚀 [极速模式] View [{view_name}] registered")
            return
        except Exception:
            pass

    if os.path.exists(proc_path):
        try:
            sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{proc_path}/*/*.parquet', hive_partitioning=true, union_by_name=true)"
            con.execute(sql)
            print(f"  🐢 [标准模式] View [{view_name}] registered")
        except Exception as e:
            print(f"  ❌ Failed: {view_name}: {e}")
    else:
        print(f"  ⚪ Path not found: {folder_name}")

views_map = {
    "stock_kline":   "stock_price_daily",
    "index_kline":   "index_price_daily",
    "etf_kline":     "etf_price_daily",
    "concept_kline": "concept_price_daily",
    "finance":       "stock_financial",
    "macro":         "macro_daily",
    "news":          "alt_cctv_news",
    "industry_pe":   "industry_pe_daily",
    "market_pe":     "market_pe_lg",
    "market_pb":     "market_pb_all"
}

print("正在注册视图...")
for view, folder in views_map.items():
    register_smart_view(view, folder)

print("-" * 80)

# ==============================================================================
# 第二步：查询演示 (查看最新一行数据)
# ==============================================================================

def print_data(title, sql):
    """通用函数：执行SQL并打印第一行数据"""
    print(f"--- {title} ---")
    try:
        # 使用 LIMIT 1 快速获取 Schema，不读取大量数据
        df = con.query(sql).df()
        cols = df.columns.tolist()
        print(f"📊 字段数量: {len(cols)}")
        print(f"📝 字段示例: {df}")
    except Exception as e:
        print(f"❌ 查询失败: {e}")
    print("\n")

# 1. 个股查询
print_data("1. 个股行情 (stock_kline)", """
    SELECT * FROM stock_kline 
    WHERE code = 'sh.600519' 
    LIMIT 1
""")

# 2. 财务查询
# 注意：finance 表字段非常多，包含 balance, income, cashflow 三张表的合集
print_data("2. 财务指标 (finance)", """
    SELECT * FROM finance 
    WHERE code = 'sz.300750' AND year >= 2023 
    LIMIT 1
""")

# 3. 指数查询
print_data("3. 指数行情 (index_kline)", """
    SELECT * FROM index_kline 
    WHERE code = 'sh.000001' 
    LIMIT 1
""")

# 4. ETF查询
print_data("4. ETF行情 (etf_kline)", """
    SELECT * FROM etf_kline 
    WHERE name = 'HS300' 
    LIMIT 1
""")

# 5. 概念查询
print_data("5. 概念板块 (concept_kline)", """
    SELECT * FROM concept_kline 
    WHERE concept_name = 'AI智能体' 
    LIMIT 1
""")

# 6. 宏观查询
print_data("6. 宏观经济 (macro)", """
    SELECT * FROM macro 
    LIMIT 1
""")

# 7. 新闻查询
print_data("7. 新闻联播 (news)", """
    SELECT * FROM news 
    LIMIT 1
""")

# 8. 行业PE查询
print_data("8. 行业PE/PB (industry_pe)", """
    SELECT * FROM industry_pe 
    LIMIT 1
""")

# 字段说明请参考文档或源码注释
