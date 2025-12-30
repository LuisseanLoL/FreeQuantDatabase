# -*- coding: utf-8 -*-
"""
全市场股票代码生成器 (包含退市股票)
路径: src/utils/universe_generator.py
功能: 
    通过回溯 Baostock 历史每年的最后交易日，抓取当时的股票列表并合并。
    从而生成一份包含"当前上市" + "历史已退市" 的完整股票代码表。
"""

import baostock as bs
import pandas as pd
import datetime
import sys
import os
from pathlib import Path
from tqdm import tqdm

# 🚑 路径补丁
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from config.settings import DATA_DIR

def generate_full_market_codes(save_path: str = None):
    """
    生成全历史股票列表
    :return: List[str] 股票代码列表
    """
    # 1. 登录
    bs.login()

    print("正在计算历史交易日历...")
    end_date = datetime.date.today().strftime('%Y-%m-%d')
    
    # 获取从 1991 年至今的所有交易日
    rs = bs.query_trade_dates(start_date="1991-01-01", end_date=end_date)
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    
    df_cal = pd.DataFrame(data_list, columns=rs.fields)
    # 筛选出交易日 (is_trading_day == '1')
    df_trade = df_cal[df_cal['is_trading_day'] == '1'].copy()
    df_trade['calendar_date'] = pd.to_datetime(df_trade['calendar_date'])
    
    # 2. 提取每年的最后一个交易日
    # 按年份分组，取最大日期
    df_trade['year'] = df_trade['calendar_date'].dt.year
    year_end_dates = df_trade.groupby('year')['calendar_date'].max().dt.strftime('%Y-%m-%d').tolist()
    
    # 补上今天 (确保包含最近上市的新股)
    if end_date not in year_end_dates:
        year_end_dates.append(end_date)

    print(f"将回溯查询以下 {len(year_end_dates)} 个历史时间点的成分股:")
    print(f"{year_end_dates[0]} ... {year_end_dates[-1]}")

    # 3. 循环查询并收集代码
    all_codes = set()
    
    for date in tqdm(year_end_dates, desc="Sampling History"):
        try:
            rs = bs.query_all_stock(day=date)
            while rs.next():
                # Baostock 返回 [code, tradeStatus, code_name]
                code = rs.get_row_data()[0]
                # 过滤指数
                if code.startswith("sh.6") or code.startswith("sz.0") or code.startswith("sz.3") or code.startswith("bj."):
                    all_codes.add(code)
        except Exception:
            pass
            
    bs.logout()
    
    # 4. 排序与保存
    sorted_codes = sorted(list(all_codes))
    print(f"\n✅ 合并完成！全历史共发现 {len(sorted_codes)} 只股票 (含退市)。")
    
    if save_path:
        df_save = pd.DataFrame({'code': sorted_codes})
        # 保存到 config 或 data 目录
        df_save.to_csv(save_path, index=False)
        print(f"💾 已保存至: {save_path}")
        
    return sorted_codes

if __name__ == "__main__":
    # 默认保存到 config 文件夹下，方便 main.py 调用
    save_file = Path(project_root) / "config" / "all_stock_list.csv"
    generate_full_market_codes(str(save_file))