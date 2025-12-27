# -*- coding: utf-8 -*-
"""
通用日志模块
路径: src/utils/logger.py
功能: 提供统一的日志配置，支持控制台输出和文件滚动记录
"""

import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

# 1. 自动确定日志存储路径
# 向上寻找项目根目录 (假设当前文件在 src/utils/ 下)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"

# 确保日志目录存在
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 定义默认日志格式
# 时间 - 模块名 - 日志级别 - 消息
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def get_logger(name: str = "System", log_filename: str = "app.log", level=logging.INFO):
    """
    获取配置好的 Logger 对象
    
    :param name: 日志记录器名称 (通常传 __name__)
    :param log_filename: 日志文件名 (默认存放在 logs/ 目录下)
    :param level: 日志级别 (logging.INFO / DEBUG / ERROR)
    :return: logger 实例
    """
    logger = logging.getLogger(name)
    
    # 防止重复添加 Handler (如果 logger 已存在且已有 handler，直接返回)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # ---------------------------
    # 1. 控制台 Handler (StreamHandler)
    # ---------------------------
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(DEFAULT_FORMAT, datefmt=DATE_FORMAT)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # ---------------------------
    # 2. 文件 Handler (RotatingFileHandler)
    # ---------------------------
    # 完整路径
    log_path = LOG_DIR / log_filename
    
    # maxBytes=10MB (10*1024*1024), backupCount=5 (保留5个历史文件)
    # encoding='utf-8' 防止中文乱码
    file_handler = RotatingFileHandler(
        log_path, 
        maxBytes=10*1024*1024, 
        backupCount=5, 
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(console_formatter)
    logger.addHandler(file_handler)

    return logger

# --- 测试代码 ---
if __name__ == "__main__":
    # 测试日志模块
    log = get_logger("TestLogger", "test.log")
    
    log.info("这是一条普通信息 (Info)")
    log.warning("这是一条警告信息 (Warning)")
    log.error("这是一条错误信息 (Error)")
    
    print(f"\n日志文件已生成于: {LOG_DIR}")