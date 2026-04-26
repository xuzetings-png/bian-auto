#!/usr/bin/env python3
"""
实盘交易测试脚本
使用最佳策略进行实盘交易
"""

import os
import sys
from decimal import Decimal
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from trading_bot.advanced_strategy import TrendFollowingStrategy
from trading_bot.advanced_executor import AdvancedExecutor
from trading_bot.logging_utils import configure_logging
from trading_bot.config import get_settings

def create_env_file():
    """创建环境文件（如果不存在）"""
    env_path = project_root / ".env"
    if not env_path.exists():
        print("⚠️  没有找到 .env 文件")
        print("请创建 .env 文件并设置以下变量：")
        print("BINANCE_API_KEY=你的API密钥")
        print("BINANCE