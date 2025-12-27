# -*- coding: utf-8 -*-
"""
Parquet 存储管理器 (精确覆盖版)
路径: src/storage/parquet_manager.py
功能: 
    1. 手动管理 Hive 分区路径
    2. 强制使用 "{code}.parquet" 命名文件，实现精确覆盖
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os
from pathlib import Path

# 🚑 路径补丁
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.logger import get_logger

logger = get_logger(__name__, "storage.log")

class ParquetStorage:
    def __init__(self, base_dir: str = "data/processed"):
        if not os.path.isabs(base_dir):
            self.base_dir = Path(project_root) / base_dir
        else:
            self.base_dir = Path(base_dir)
            
        if not self.base_dir.exists():
            self.base_dir.mkdir(parents=True, exist_ok=True)

    def save_partitioned(self, 
                         df: pd.DataFrame, 
                         category: str, 
                         partition_col: str = 'date',
                         key_col: str = 'code'):
        """
        保存数据，强制文件名为 "{key_col}.parquet" (例如 sh.600000.parquet)
        
        :param df: 数据 DataFrame
        :param category: 数据类别 (如 stock_price_daily)
        :param partition_col: 时间列，用于提取年份 (如 date)
        :param key_col: 用于命名的关键列 (如 code, name)
        """
        if df.empty: return

        if partition_col not in df.columns:
            logger.error(f"❌ Partition col '{partition_col}' missing")
            return
        
        if key_col not in df.columns:
            logger.error(f"❌ Key col '{key_col}' missing (needed for filename)")
            return

        # 1. 预处理：提取年份
        temp_date = pd.to_datetime(df[partition_col], errors='coerce')
        df = df.copy()
        df['year'] = temp_date.dt.year
        df = df.dropna(subset=['year'])
        df['year'] = df['year'].astype(int)

        # 2. 按年份分组处理 (通常传入的df是单只股票多年的数据)
        # 这样可以正确地把 2024年的数据存入 year=2024, 2025年的存入 year=2025
        for year, group in df.groupby('year'):
            # 目标文件夹: data/processed/stock_price_daily/year=2025
            year_dir = self.base_dir / category / f"year={year}"
            if not year_dir.exists():
                year_dir.mkdir(parents=True, exist_ok=True)

            # 3. 构造确定的文件名
            # 获取该组数据的唯一标识 (例如 sh.600000)
            # 我们假设传入的 df 是一只股票的数据，所以直接取第一行的 code
            unique_key = str(group[key_col].iloc[0])
            
            # 文件名: sh.600000.parquet
            # 注意处理文件名中可能存在的非法字符 (如 : / \)
            safe_filename = unique_key.replace("/", "_").replace("\\", "_") + ".parquet"
            file_path = year_dir / safe_filename

            try:
                # 4. 写入 (PyArrow 会直接覆盖同名文件)
                table = pa.Table.from_pandas(group)
                pq.write_table(table, file_path, compression='snappy')
                
                # logger.info(f"💾 Saved {safe_filename} to year={year}") # 日志太刷屏可注释
                
            except Exception as e:
                logger.error(f"❌ Failed to write {file_path}: {e}")