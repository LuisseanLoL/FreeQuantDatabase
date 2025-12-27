# -*- coding: utf-8 -*-
"""
日期处理工具箱
路径: src/utils/date_utils.py
依赖: akshare, pandas
"""
import akshare as ak
import pandas as pd
import datetime

def get_latest_trading_date() -> str:
    """
    获取最近的一个交易日日期 (YYYY-MM-DD)。
    利用新浪数据源判断：
    - 如果今天是交易日，返回今天；
    - 如果今天是周末/节假日，返回最近的一个之前的交易日。
    """
    try:
        # 获取交易日历 DataFrame，包含列 ['trade_date']
        df = ak.tool_trade_date_hist_sina()
        
        # 确保转为 date 类型以便比较
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        today = datetime.date.today()
        
        # 筛选出小于等于今天的交易日
        # tail(1) 取最后一个即为离今天最近的
        valid_dates = df[df['trade_date'] <= today]
        
        if valid_dates.empty:
            # 极少见情况（如早于1990年），返回今天兜底
            return today.strftime('%Y-%m-%d')
        
        latest_date = valid_dates.iloc[-1]['trade_date']
        
        return latest_date.strftime('%Y-%m-%d')
        
    except Exception as e:
        print(f"⚠️ 获取交易日历失败，使用今日日期兜底: {e}")
        return datetime.date.today().strftime('%Y-%m-%d')

# --- 简单测试 ---
if __name__ == "__main__":
    print(f"最近交易日: {get_latest_trading_date()}")