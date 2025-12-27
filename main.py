# -*- coding: utf-8 -*-
"""
AlphaFactorLab 数据更新主程序 (高度解耦版)
功能: 调度各个 Fetcher，清洗数据，并存储为 Hive Partition Parquet
用法示例:
    python main.py --mode update --task concept     <-- 仅更新概念板块
    python main.py --mode update --task index       <-- 仅更新大盘指数
    python main.py --mode update --task stock       <-- 更新个股行情
"""

import argparse
import datetime
import time
from tqdm import tqdm
from typing import Tuple
import warnings
warnings.filterwarnings("ignore")

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
    """计算时间范围: update模式回溯到当年1月1日"""
    end_date = datetime.date.today().strftime('%Y-%m-%d')
    if mode == 'full':
        start_date = START_DATE_FULL
    else:
        current_year = datetime.date.today().year
        start_date = f"{current_year}-01-01"
    return start_date, end_date

# ==========================================
# 1. 📈 指数 (Index) - [独立拆分]
# ==========================================
def run_index_update(mode: str):
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting INDEX update ({mode}): {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    
    with BaostockFetcher() as bs:
        logger.info(f"Updating {len(INDEX_POOL)} Indexes...")
        for code in INDEX_POOL:
            # 获取指数日线 (不复权)
            df = bs.fetch_index_kline(code, start_date, end_date)
            if not df.empty:
                df = cleaner.clean_daily_market_data(df)
                storage.save_partitioned(df, "index_price_daily", key_col='code')

# ==========================================
# 2. 📈 个股 (Stock)
# ==========================================
def run_stock_update(mode: str):
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting STOCK update ({mode}): {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    
    with BaostockFetcher() as bs:
        raw_codes = bs.fetch_all_stock_codes()
        # 严格过滤，只保留个股
        stock_codes = [c for c in raw_codes if not (c.startswith("sh.000") or c.startswith("sz.399"))]
        logger.info(f"Found {len(stock_codes)} stocks.")
        
        for code in tqdm(stock_codes, desc="Stocks"):
            try:
                # adjust='1' 后复权
                df = bs.fetch_daily_kline(code, start_date, end_date, adjust='1')
                if not df.empty:
                    df = cleaner.clean_daily_market_data(df)
                    storage.save_partitioned(df, "stock_price_daily", key_col='code')
            except Exception as e:
                logger.error(f"Failed stock {code}: {e}")

# ==========================================
# 3. 📊 ETF (Mootdx)
# ==========================================
def run_etf_update(mode: str):
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting ETF update ({mode}): {start_date} -> {end_date}")
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    
    with MootdxFetcher() as mdx:
        for name, (code, ipo_year) in tqdm(ETF_POOL.items(), desc="ETFs"):
            try:
                df = mdx.fetch_etf_daily_kline(code, ipo_year, start_date, end_date, adjust_factor='02')
                if not df.empty:
                    df['name'] = name 
                    df = cleaner.clean_daily_market_data(df)
                    storage.save_partitioned(df, "etf_price_daily", key_col='name')
            except Exception as e:
                logger.error(f"Failed ETF {name}: {e}")

# ==========================================
# 4. 💰 财务报表 (Finance)
# ==========================================
def run_finance_update(mode: str):
    """仅更新财务报表，不再包含概念数据"""
    logger.info(f"🚀 Starting FINANCIAL REPORT update")
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    ak_fetcher = AkshareFetcher()
    
    # 借助 Baostock 获取最新的股票列表
    with BaostockFetcher() as bs:
        raw_codes = bs.fetch_all_stock_codes()
        stock_codes = [c for c in raw_codes if not (c.startswith("sh.000") or c.startswith("sz.399"))]
        
    logger.info(f"Updating Financial Reports for {len(stock_codes)} stocks...")
    for code in tqdm(stock_codes, desc="Finance"):
        try:
            df = ak_fetcher.fetch_financial_report(code)
            if not df.empty:
                df = cleaner.clean_financial_report(df)
                storage.save_partitioned(df, "stock_financial", partition_col="report_date", key_col='code')
        except: pass

# ==========================================
# 5. 💡 概念板块 (Concept) - [独立拆分]
# ==========================================
def run_concept_update(mode: str):
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting CONCEPT update ({mode}): {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    ak_fetcher = AkshareFetcher()
    
    df_concepts = ak_fetcher.fetch_concept_boards()
    if df_concepts.empty:
        logger.warning("No concept boards found.")
        return

    # Akshare 概念接口通常需要 YYYYMMDD
    start_str = start_date.replace("-", "")
    end_str = end_date.replace("-", "")
    
    logger.info(f"Updating {len(df_concepts)} Concept Boards...")
    for _, row in tqdm(df_concepts.iterrows(), total=len(df_concepts), desc="Concept"):
        name = row['name']
        try:
            df_daily = ak_fetcher.fetch_concept_daily(name, start_str, end_str)
            if not df_daily.empty:
                df_daily['concept_name'] = name
                df_daily = cleaner.clean_daily_market_data(df_daily)
                storage.save_partitioned(df_daily, "concept_price_daily", key_col='concept_name')
            
            # 适当限流，同花顺接口较为敏感
            time.sleep(0.3) 
        except: pass

# ==========================================
# 6. 🗞️ 另类数据 (Alternative)
# ==========================================

def run_alt_news(mode: str):
    """仅更新新闻联播"""
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting ALT: CCTV News update: {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    ak_fetcher = AkshareFetcher()
    
    start = datetime.datetime.strptime('2016-03-30', "%Y-%m-%d") 
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days + 1)]
    
    for date_obj in tqdm(date_generated, desc="CCTV News"):
        date_str = date_obj.strftime("%Y%m%d")
        try:
            df_news = ak_fetcher.fetch_cctv_news(date_str)
            if not df_news.empty:
                df_news = cleaner.clean_news_data(df_news)
                storage.save_partitioned(df_news, "alt_cctv_news", key_col='date')
        except: pass

def run_alt_industry_pe(mode: str):
    """仅更新行业市盈率"""
    start_date, end_date = get_date_range(mode)
    logger.info(f"🚀 Starting ALT: Industry PE update: {start_date} -> {end_date}")
    
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    ak_fetcher = AkshareFetcher()
    
    start = datetime.datetime.strptime('2023-05-19', "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days + 1)]
    
    for date_obj in tqdm(date_generated, desc="Industry PE"):
        date_str = date_obj.strftime("%Y%m%d")
        try:
            df_pe = ak_fetcher.fetch_industry_pe_snapshot(date_str)
            if not df_pe.empty:
                if '变动日期' in df_pe.columns:
                    df_pe.rename(columns={'变动日期': 'date'}, inplace=True)
                    df_pe = cleaner.normalize_date(df_pe)
                    storage.save_partitioned(df_pe, "industry_pe_daily", key_col='date')
        except: pass

def run_alt_market_metric(mode: str):
    """更新全市场估值 (PE/PB)"""
    logger.info(f"🚀 Starting ALT: Market Metrics (PE/PB) update")
    storage = ParquetStorage(PROCESSED_DIR)
    cleaner = DataCleaner()
    ak_fetcher = AkshareFetcher()
    
    try:
        df_pe = ak_fetcher.fetch_market_pe()
        if not df_pe.empty and 'date' in df_pe.columns:
            df_pe = cleaner.normalize_date(df_pe)
            storage.save_partitioned(df_pe, "market_pe_lg", key_col='date')

        df_pb = ak_fetcher.fetch_market_pb()
        if not df_pb.empty and 'date' in df_pb.columns:
            df_pb = cleaner.normalize_date(df_pb)
            storage.save_partitioned(df_pb, "market_pb_all", key_col='date')
    except Exception as e:
        logger.error(f"Failed to update market metrics: {e}")

def run_alt_all(mode: str):
    run_alt_news(mode)
    run_alt_industry_pe(mode)
    run_alt_market_metric(mode)

# ==========================================
# 主入口
# ==========================================
if __name__ == "__main__":
    # 定义支持的任务列表
    TASKS = [
        'all',             # 跑所有
        'stock',           # 个股行情
        'index',           # [新增] 指数行情
        'etf',             # ETF行情
        'finance',         # 财务报表
        'concept',         # [新增] 概念板块
        'alt',             # 所有另类数据
        'alt_news',        # 仅新闻
        'alt_industry_pe', # 仅行业PE
        'alt_market_metric'# 仅市场估值
    ]

    parser = argparse.ArgumentParser(description="AlphaFactorLab Data Updater")
    parser.add_argument('--mode', type=str, choices=['full', 'update'], default='update', help='full: 全量, update: 当年增量')
    parser.add_argument('--task', type=str, choices=TASKS, default='all', help='指定运行的任务')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    # 任务调度逻辑
    if args.task == 'all':
        run_index_update(args.mode) # 先跑指数，速度快
        run_stock_update(args.mode)
        run_etf_update(args.mode)
        run_finance_update(args.mode)
        run_concept_update(args.mode)
        run_alt_all(args.mode)
    
    elif args.task == 'index':
        run_index_update(args.mode)

    elif args.task == 'stock':
        run_stock_update(args.mode)
        
    elif args.task == 'etf':
        run_etf_update(args.mode)
        
    elif args.task == 'finance':
        run_finance_update(args.mode)
        
    elif args.task == 'concept':
        run_concept_update(args.mode)
        
    elif args.task == 'alt':
        run_alt_all(args.mode)
        
    elif args.task == 'alt_news':
        run_alt_news(args.mode)
        
    elif args.task == 'alt_industry_pe':
        run_alt_industry_pe(args.mode)
        
    elif args.task == 'alt_market_metric':
        run_alt_market_metric(args.mode)
        
    elapsed = time.time() - start_time
    logger.info(f"🎉 Task '{args.task}' completed in {elapsed:.2f} seconds.")