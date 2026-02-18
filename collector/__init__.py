# -*- coding: utf-8 -*-
"""
Prometheus Collector 模块

功能：
- 实现 Prometheus 指标收集逻辑
- 协调各个 Provider 收集配额数据
- 暴露 Prometheus 格式的指标
"""

from .collector import QuotaCollector
from .quota_result import QuotaResult, QuotaStatus

