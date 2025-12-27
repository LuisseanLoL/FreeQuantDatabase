# -*- coding: utf-8 -*-
"""
DuckDB 数据库连接器
路径: src/storage/db_connector.py
功能: 
    1. 管理 DuckDB 连接
    2. 将 Hive Partition 结构的 Parquet 文件注册为数据库视图
    3. 提供 SQL 查询接口，返回 DataFrame
"""

import duckdb
import pandas as pd
import os
import sys
from pathlib import Path
from typing import Optional, List

# 🚑 路径补丁
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

# 引入日志配置
from src.utils.logger import get_logger

logger = get_logger(__name__, "db_connector.log")

class DuckDBConnector:
    def __init__(self, db_path: str = "quant_data.duckdb", read_only: bool = False):
        """
        初始化连接器
        :param db_path: 数据库文件路径 (默认为项目根目录下的 quant_data.duckdb)
        :param read_only: 是否只读模式 (用于分析脚本，防止锁库)
        """
        # 确保路径是绝对路径，或者相对于项目根目录
        if not os.path.isabs(db_path):
            self.db_path = os.path.join(project_root, db_path)
        else:
            self.db_path = db_path
            
        self.read_only = read_only
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        """建立数据库连接"""
        try:
            self.conn = duckdb.connect(database=self.db_path, read_only=self.read_only)
            
            # --- 修正点: 移除过时的全局配置 SET hive_partitioning=true ---
            # 新版 DuckDB 在 read_parquet 函数中直接指定即可，无需全局开启
            
            logger.info(f"✅ DuckDB connected: {self.db_path} (ReadOnly={self.read_only})")
        except Exception as e:
            logger.error(f"❌ DuckDB connection failed: {e}")
            raise e

    def disconnect(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("👋 DuckDB disconnected")

    def create_view_from_parquet(self, view_name: str, parquet_dir: str):
        """
        [核心功能] 将 Parquet 文件夹注册为视图
        """
        if not self.conn:
            self.connect()

        # 处理路径：确保是通配符路径
        path_obj = Path(parquet_dir)
        if not path_obj.is_absolute():
            path_obj = Path(project_root) / parquet_dir
        
        # 构造 glob 模式：匹配该目录下任意层级的所有 parquet 文件
        # 注意：DuckDB 需要正斜杠路径
        glob_path = path_obj.as_posix() + "/**/*.parquet"
        
        try:
            # 在这里指定 hive_partitioning=true 是正确的做法
            sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{glob_path}', hive_partitioning=true);"
            self.conn.execute(sql)
            logger.info(f"✅ View created: {view_name} -> {glob_path}")
        except Exception as e:
            logger.error(f"❌ Failed to create view {view_name}: {e}")

    def query(self, sql: str, params: Optional[List] = None) -> pd.DataFrame:
        """执行查询并返回 DataFrame"""
        if not self.conn:
            self.connect()
            
        try:
            if params:
                return self.conn.execute(sql, params).df()
            else:
                return self.conn.execute(sql).df()
        except Exception as e:
            logger.error(f"❌ Query failed: {sql} | Error: {e}")
            return pd.DataFrame()

    def list_tables(self) -> pd.DataFrame:
        """列出当前数据库中的所有表和视图"""
        return self.query("SHOW TABLES")

    def get_schema(self, table_name: str) -> pd.DataFrame:
        """查看表结构"""
        return self.query(f"DESCRIBE {table_name}")

# ==========================================
# 测试代码
# ==========================================
if __name__ == "__main__":
    test_db = "test_quant.duckdb"
    
    with DuckDBConnector(test_db) as db:
        print("1. 连接成功，准备注册视图...")
        
        # 假设的数据路径
        stock_daily_path = "data/processed/stock_price_daily"
        
        # 即使文件夹为空或不存在，register view 通常也不会立即报错，只有查询时才会报错
        db.create_view_from_parquet("stock_daily", stock_daily_path)
        
        print("\n2. 查看现有表:")
        print(db.list_tables())

    # 清理测试生成的 db 文件
    if os.path.exists(os.path.join(project_root, test_db)):
        try:
            os.remove(os.path.join(project_root, test_db))
            print("\n测试清理完成。")
        except:
            pass