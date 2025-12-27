# 🚀 FreeQuantDatabase: 本地化 A 股量化数据库

![Python](https://img.shields.io/badge/Python-3.9%2B-blue) ![License](https://img.shields.io/badge/License-MIT-green) ![DuckDB](https://img.shields.io/badge/Database-DuckDB-yellow) ![Data](https://img.shields.io/badge/Data-Parquet-orange)

**FreeQuantDatabase** 是一个**完全免费、开源、无需繁琐注册**的 A 股量化数据解决方案。

旨在为个人宽客（Quants）提供一套标准化的 **ETL（提取、转换、加载）** 流程，将散落在互联网各个免费角落的金融数据（Baostock, Akshare, Mootdx 等）清洗并整合为高性能的 **Parquet** 文件，并通过 **DuckDB** 实现极速查询。

> **核心理念**：数据应该像空气一样免费。拒绝高昂的终端费用，构建属于你自己的本地金融数据湖。感谢Baostock, Akshare, Mootdx 等开发者提供的开源接口！

---

## ✨ 核心特性

*   **💸 零成本 & 无门槛**：
    *   无需购买 Wind/Choice 账号。
    *   无需申请复杂的 API Token。
    *   集成 **Baostock** (证券宝)、**Akshare** (开源财经)、**Mootdx** (通达信协议) 三大免费源。
*   **🏗️ 专业级存储架构**：
    *   采用 **Parquet** 列式存储，压缩率极高（全量历史数据仅需数 GB）。
    *   遵循 **Hive Partitioning** (`year=2025/sh.600000.parquet`)，支持大数据量下的秒级分区裁剪。
    *   支持 **DuckDB** "零拷贝" 挂载，像查询 SQL 一样查询文件。
*   **🔄 幂等增量更新**：
    *   独创的 **"按年覆盖" (Partition Overwrite)** 更新策略。
    *   每天运行一次 Update，自动修正当年所有历史数据（解决昨收修正、除权除息回溯问题），且天然去重，无需担心重复数据。
*   **🧩 模块化设计**：
    *   Fetcher (采集) -> Processor (清洗) -> Storage (存储) 职责分离，易于扩展。

---

## 📚 数据支持

| 数据类型 | 数据源 | 频率 | 说明 |
| :--- | :--- | :--- | :--- |
| **📈 个股行情** | Baostock | 日频 | 包含开高低收、成交量、换手率、复权因子等 (1990年至今) |
| **📊 指数行情** | Baostock | 日频 | 上证/深证/创业/科创/沪深300等主流指数 |
| **💰 ETF 行情** | Mootdx | 日频 | 基于通达信协议，包含宽基、行业、跨境、商品 ETF |
| **📑 财务报表** | Akshare | 季频 | 营收、净利润、ROE、负债率等 30+ 核心财务指标 |
| **💡 概念板块** | Akshare | 日频 | 同花顺概念板块指数及成分股 |
| **📰 另类数据** | Akshare | 日频 | 新闻联播文字稿 (NLP情感分析)、行业市盈率 (PE/PB) |

---

## 🛠️ 安装指南

### 1. 克隆项目
```bash
git clone https://github.com/yourname/FreeQuantDatabase.git
cd FreeQuantDatabase
```

### 2. 创建虚拟环境 (推荐)
```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# Mac/Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. 安装依赖
```bash
pip install -r requirements.txt
```

---

## 🏃‍♂️ 运行说明

项目通过 `main.py` 统一调度。优先使用后复权数据以避免增量更新时数据无法对齐。

### 1. 全量初始化 (首次运行)
下载 1990 年至今的所有历史数据。耗时较长（视网络情况约 1-2 小时），建议挂机运行。
```bash
python main.py --mode full --task all
```

### 2. 日常增量更新 (每日收盘后)
下载 **当年 (Current Year)** 的所有数据并覆盖当年的分区文件。
*   **特点**：速度快，且能自动修正近期可能变动的历史数据。
```bash
python main.py --mode update --task all
```

### 3. 分任务运行
如果你只想更新某类数据：
```bash
python main.py --mode update --task stock    # 仅更新股票
python main.py --mode update --task etf      # 仅更新ETF
python main.py --mode update --task finance  # 仅更新财报
```

---

## 🔎 如何使用数据 (DuckDB)

数据下载后存储在 `data/processed/` 目录下。你可以直接读取 Parquet，或者使用 DuckDB 进行 SQL 查询。

**Python 示例代码：**

```python
import duckdb

# 1. 建立连接 (内存模式或文件模式)
con = duckdb.connect()

# 2. 注册视图 (自动识别 hive 分区)
# 注意：*.parquet 会自动递归搜索所有年份文件夹
con.execute("""
    CREATE VIEW stock_kline AS 
    SELECT * FROM read_parquet('data/processed/stock_price_daily/*/*.parquet', hive_partitioning=true)
""")

# 3. 执行 SQL 查询
# 查询 贵州茅台 (sh.600519) 2023年以来的收盘价和PE
df = con.query("""
    SELECT date, code, close, peTTM 
    FROM stock_kline 
    WHERE code = 'sh.600519' 
      AND year >= 2023
    ORDER BY date
""").df()

print(df)
```

---

## 📂 目录结构

```text
FreeQuantDatabase/
├── config/                 # 配置文件 (资产池、路径)
├── data/                   # 数据存储目录 (已加入 .gitignore)
│   └── processed/          # 清洗后的 Parquet 文件
│       ├── stock_price_daily/
│       │   └── year=2025/  # Hive 分区
│       │       └── sh.600000.parquet
│       ├── stock_financial/
│       └── ...
├── logs/                   # 运行日志
├── src/                    # 源代码
│   ├── fetchers/           # 爬虫/API 接口 (Baostock, Akshare, Mootdx)
│   ├── processors/         # 数据清洗 (清洗字段、日期对齐)
│   ├── storage/            # 存储管理 (Parquet 读写、DuckDB 连接)
│   └── utils/              # 通用工具 (日志、日期计算)
├── main.py                 # 程序主入口
└── requirements.txt        # 依赖库
```

---

## 📝 常见问题 (FAQ)

**Q: 为什么生成的 Parquet 文件名是 `sh.600000.parquet` 而不是随机乱码？**
A: 为了支持 **幂等更新**。通过以代码命名，我们可以在 Update 模式下直接覆盖对应的文件，而不需要删除整个文件夹。这使得系统支持断点续传，且不会产生重复数据。

**Q: Baostock 提示登录失败？**
A: Baostock 即使不注册也能使用（匿名登录），代码内部已处理。如果报错网络问题，通常是服务器波动，稍后重试即可。

**Q: 数据更新频率是多少？**
A: 建议在每个交易日 **18:00** 以后运行更新脚本，以确保各大源的数据已归档。

---

## ⚖️ 免责声明

本项目仅供学习和研究使用，数据来源于互联网公开免费接口。
*   使用者应自行核实数据的准确性。
*   严禁将本项目用于任何商业用途或非法用途。
*   投资有风险，入市需谨慎。本项目不构成任何投资建议。