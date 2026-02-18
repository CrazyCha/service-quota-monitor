# -*- coding: utf-8 -*-
"""
定时任务模块

功能：
- 根据 cache_ttl_limit 和 cache_ttl_usage 定时刷新配额数据
- 在后台线程中运行，不阻塞主程序
"""

from scheduler.scheduler import QuotaScheduler

__all__ = ['QuotaScheduler']






