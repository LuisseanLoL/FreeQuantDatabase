# 🏦 FreeQuantDatabase

![Python](https://img.shields.io/badge/Python-3.9%2B-blue) ![License](https://img.shields.io/badge/License-MIT-green) ![DuckDB](https://img.shields.io/badge/Database-DuckDB-yellow) ![Status](https://img.shields.io/badge/Status-Production-success)

**FreeQuantDatabase** 是一个完全免费、本地化、高性能的 A 股量化数据仓库解决方案。

它不包含任何策略逻辑，只专注于一件事：**将互联网上散乱的免费金融数据（Baostock, Akshare, Mootdx），清洗并结构化为高性能的 Parquet 数据湖，供下游的回测或因子系统调用。感谢Baostock, Akshare, Mootdx 等开发者提供的开源接口！**

---

## ✨ 核心特性

*   **🧱 纯粹的数据基座**：解耦策略与数据，只做 ETL (Extract, Transform, Load)。
*   **💸 永久免费**：基于 Baostock、Akshare、Mootdx 等开源接口，零成本构建本地数据中心。
*   **⚡ 极速存储**：
    *   **Parquet + Snappy**：高压缩比列式存储。
    *   **Hive Partitioning**：按年份分区，支持秒级查询过滤。
    *   **DuckDB Ready**：支持“零拷贝”SQL 查询，无缝对接 Pandas。
*   **🔄 幂等增量更新**：
    *   采用 **"按年文件名覆盖"** 策略。
    *   每日运行 Update 模式，自动刷新当年数据，天然去重，支持断点续传。

---

## 📊 数据资产清单

| 类别 | 存储路径 (data/processed/...) | 关键字段 (Key) | 数据源 |
| :--- | :--- | :--- | :--- |
| **个股日线** | `stock_price_daily` | `code` (如 sh.600519) | Baostock (后复权) |
| **指数日线** | `index_price_daily` | `code` (如 sh.000001) | Baostock |
| **ETF日线** | `etf_price_daily` | `name` (如 HS300) | Mootdx (通达信) |
| **财务报表** | `stock_financial` | `code` (按报告期分区) | Akshare (同花顺源) |
| **概念板块** | `concept_price_daily` | `concept_name` | Akshare (同花顺源) |
| **另类数据** | `alt_cctv_news` | `date` | 新闻联播文字稿 |
| **另类数据** | `industry_pe_daily` | `date` | 证监会行业市盈率 |
| **另类数据** | `market_pe_lg` | `date` | A股全市场PE估值 |

---

## 🚀 快速开始

### 1. 环境准备
```bash
git clone https://github.com/yourname/FreeQuantDatabase.git
cd FreeQuantDatabase
pip install -r requirements.txt
```

### 2. 全量初始化 (下载 1990年至今数据)
*建议在网络良好的环境下挂机运行。*
```bash
python main.py --mode full --task all
```

### 3. 日常更新 (每日收盘后)
*自动回溯并覆盖当年的数据，修正昨收盘及除权信息。*
```bash
python main.py --mode update --task all
```

### 4. 细粒度任务控制
```bash
python main.py --mode update --task stock            # 仅更新个股
python main.py --mode update --task finance          # 仅更新财报
python main.py --mode update --task alt_industry_pe  # 仅更新行业PE
```

---

## 🔗 如何在其他项目中调用？

**FreeQuantDatabase** 产生的 `data/processed` 文件夹就是一个标准的 **Data Lake**。
你不需要在策略项目中引用本项目的 Python 代码，只需使用 **DuckDB** 直连文件夹。

**示例：在你的回测项目 (`MyBacktest`) 中读取数据**

```python
import duckdb

# 指向 FreeQuantDatabase 的数据目录
DB_PATH = "E:/Projects/FreeQuantDatabase/data/processed"

# 1. 连接并注册视图 (View)
con = duckdb.connect()
con.execute(f"""
    CREATE VIEW stock_kline AS 
    SELECT * FROM read_parquet('{DB_PATH}/stock_price_daily/*/*.parquet', hive_partitioning=true);
    
    CREATE VIEW finance AS 
    SELECT * FROM read_parquet('{DB_PATH}/stock_financial/*/*.parquet', hive_partitioning=true);
""")

# 2. 像查询数据库一样使用
# 获取 贵州茅台 2024年的量价数据
df_price = con.query("SELECT * FROM stock_kline WHERE code='sh.600519' AND year=2024").df()

# 获取 2024年报的 ROE
df_fund = con.query("SELECT * FROM finance WHERE code='600519' AND year=2024").df()
```

---

## 🛠️ 维护与贡献

*   **数据清洗逻辑**：位于 `src/processors/cleaner.py`，可根据需求调整字段映射。
*   **资产池配置**：位于 `config/settings.py`，可添加新的 ETF 或指数。

---

## ⚖️ License

MIT License. 
本项目仅供学习研究，数据来源于公开网络接口，使用者需自行核实数据准确性。
```