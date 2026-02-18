# -*- coding: utf-8 -*-
"""
Provider Discovery 模块

功能：
- 抽象 AccountProvider 和 RegionProvider 接口
- 提供 CMDB Provider 实现（从 MySQL 数据库读取账号和区域）
"""

from .interfaces import AccountProvider, RegionProvider
from .cmdb_provider import CMDBAccountProvider, CMDBRegionProvider

__all__ = [
    'AccountProvider',
    'RegionProvider',
    'CMDBAccountProvider',
    'CMDBRegionProvider',
]






