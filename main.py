# -*- coding: utf-8 -*-
"""
AlphaFactorLab 数据更新主程序 (精确覆盖版)
功能: 调度各个 Fetcher，清洗数据，并存储为 Hive Partition Parquet
特点: 使用 {key_col}.parquet 命名文件，支持幂等写入，无需清空文件夹即可去重
用法:
    python main.py --task all --mode update
    python main.py --task stock --mode full
"""

import argparse
import datetime
import time
from tqdm import tqdm
from typing import Tuple

# --- 导入配置 ---
from config.settings import ETF_POOL, INDEX_POOL, START_DATE_FULL, PROCESSED_DIR

# 引入核心模块
from src.fetchers.baostock_api import BaostockFetcher
from src.fetchers.akshare_api import AkshareFetcher
from src.fetchers.mootdx_api import MootdxFetcher
from src.processors.cleaner import DataCleaner
from src.storage.parquet_manager import ParquetStorage
from src.utils.logger import get_logger

# 配置日志
logger = get_logger("Main", "data_update.log")

def get_date_range(mode: str) -> Tuple[str, str]:
    """
    计算时间范围
    full: 1990-12-19 -> 今天
    update: 今年1月1日 -> 今天 
            (配合文件名覆盖策略，每次更新重跑当年数据，确保数据修正且无重复)
    """
    end_date = datetime.date.today().strftime('%Y-%m-%d')
    
    if mode == 'full':
        start_date = START_DATE_FULL
    else:
        # update 模式: 获取当年的1月1日
        current_year = datetime.date.today().year
        start_date = f"{current_year}-01-01"
        
    return start_date, end_date

# ==========================================
# 1. 📈 股票与指数 (Baostock)
# ==========================================
def run_stock_update(mode: str):
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting STOCK update ({mode}): {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    
    with BaostockFetcher() as bs:
        # 1.1 更新指数 (Index)
        logger.info("Updating Indexes...")
        for code in INDEX_POOL:
            # 获取指数日线 (不复权)
            df = bs.fetch_index_kline(code, start_date, end_date)
            if not df.empty:
                df = cleaner.clean_daily_market_data(df)
                # 指数按代码命名: sh.000001.parquet
                storage.save_partitioned(df, "index_price_daily", key_col='code')
        
        # 1.2 更新个股 (Stock)
        raw_codes = bs.fetch_all_stock_codes()
        
        # --- 过滤指数代码 ---
        # 剔除 'sh.000' 和 'sz.399' 开头的指数
        stock_codes = [
            c for c in raw_codes 
            if not (c.startswith("sh.000") or c.startswith("sz.399"))
        ]
        logger.info(f"Found {len(raw_codes)} codes, filtered to {len(stock_codes)} stocks.")
        
        for code in tqdm(stock_codes, desc="Stocks"):
            try:
                # adjust='1' 表示后复权
                df = bs.fetch_daily_kline(code, start_date, end_date, adjust='1')
                if not df.empty:
                    df = cleaner.clean_daily_market_data(df)
                    # 个股按代码命名: sh.600000.parquet
                    # 即使多次运行，同名文件会被覆盖，实现天然去重
                    storage.save_partitioned(df, "stock_price_daily", key_col='code')
            except Exception as e:
                logger.error(f"Failed stock {code}: {e}")

# ==========================================
# 2. 📊 ETF (Mootdx)
# ==========================================
def run_etf_update(mode: str):
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting ETF update ({mode}): {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    
    with MootdxFetcher() as mdx:
        for name, (code, ipo_year) in tqdm(ETF_POOL.items(), desc="ETFs"):
            try:
                # adjust_factor='02' 表示后复权
                df = mdx.fetch_etf_daily_kline(code, ipo_year, start_date, end_date, adjust_factor='02')
                if not df.empty:
                    df['name'] = name 
                    df = cleaner.clean_daily_market_data(df)
                    # ETF 按名称命名: HS300.parquet (直观易读)
                    storage.save_partitioned(df, "etf_price_daily", key_col='name')
            except Exception as e:
                logger.error(f"Failed ETF {name}: {e}")

# ==========================================
# 3. 💰 财务与概念 (Akshare)
# ==========================================
def run_finance_update(mode: str):
    logger.info(f"🚀 Starting FINANCE & CONCEPT update")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    ak_fetcher = AkshareFetcher()
    
    # 3.1 财务数据
    with BaostockFetcher() as bs:
        raw_codes = bs.fetch_all_stock_codes()
        stock_codes = [c for c in raw_codes if not (c.startswith("sh.000") or c.startswith("sz.399"))]
        
    logger.info(f"Updating Financial Reports for {len(stock_codes)} stocks...")
    for code in tqdm(stock_codes, desc="Finance"):
        try:
            # 财报通常返回历史所有数据，所以 mode 参数影响不大，总是全量覆盖单股文件
            df = ak_fetcher.fetch_financial_report(code)
            if not df.empty:
                df = cleaner.clean_financial_report(df)
                # 按报告期年份分区，文件名为 code.parquet (e.g. year=2023/sh.600000.parquet)
                storage.save_partitioned(df, "stock_financial", partition_col="report_date", key_col='code')
        except Exception:
            pass

    # 3.2 概念板块
    logger.info("Updating Concepts...")
    df_concepts = ak_fetcher.fetch_concept_boards()
    if not df_concepts.empty:
        start_date, end_date = get_date_range(mode)
        start_str = start_date.replace("-", "")
        end_str = end_date.replace("-", "")
        
        for _, row in tqdm(df_concepts.iterrows(), total=len(df_concepts), desc="Concept Daily"):
            name = row['name']
            try:
                df_daily = ak_fetcher.fetch_concept_daily(name, start_str, end_str)
                if not df_daily.empty:
                    df_daily['concept_name'] = name
                    df_daily = cleaner.clean_daily_market_data(df_daily)
                    # 概念按名称命名: 锂电池.parquet
                    storage.save_partitioned(df_daily, "concept_price_daily", key_col='concept_name')
                time.sleep(0.5) 
            except Exception:
                pass

# ==========================================
# 4. 🗞️ 另类数据
# ==========================================
def run_alt_update(mode: str):
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting ALTERNATIVE update: {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    ak_fetcher = AkshareFetcher()
    
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days + 1)]
    
    for date_obj in tqdm(date_generated, desc="Daily Alt Data"):
        date_str = date_obj.strftime("%Y%m%d")
        
        # 4.1 新闻联播 (按日存储)
        try:
            df_news = ak_fetcher.fetch_cctv_news(date_str)
            if not df_news.empty:
                df_news = cleaner.clean_news_data(df_news)
                # 这里 df_news 只有当天数据，key_col='date' 会生成如 2025-12-27.parquet
                storage.save_partitioned(df_news, "alt_cctv_news", key_col='date')
        except: pass
            
        # 4.2 行业市盈率 (按日存储)
        try:
            df_pe = ak_fetcher.fetch_industry_pe_snapshot(date_str)
            if not df_pe.empty:
                if '变动日期' in df_pe.columns:
                    df_pe.rename(columns={'变动日期': 'date'}, inplace=True)
                    df_pe = cleaner.normalize_date(df_pe)
                    # 生成如 2025-12-27.parquet
                    storage.save_partitioned(df_pe, "industry_pe_daily", key_col='date')
        except: pass

# ==========================================
# 主入口
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AlphaFactorLab Data Updater")
    parser.add_argument('--mode', type=str, choices=['full', 'update'], default='update', help='full: 全量历史, update: 重跑当年数据')
    parser.add_argument('--task', type=str, choices=['all', 'stock', 'etf', 'finance', 'alt'], default='all', help='指定运行的任务')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    if args.task in ['all', 'stock']:
        run_stock_update(args.mode)
        
    if args.task in ['all', 'etf']:
        run_etf_update(args.mode)
        
    if args.task in ['all', 'finance']:
        run_finance_update(args.mode)
        
    if args.task in ['all', 'alt']:
        run_alt_update(args.mode)
        
    elapsed = time.time() - start_time
    logger.info(f"🎉 All tasks completed in {elapsed:.2f} seconds.")