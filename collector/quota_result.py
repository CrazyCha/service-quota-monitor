# -*- coding: utf-8 -*-
"""
配额采集结果数据结构

功能：
- 定义配额采集结果的状态和原因
- 统一管理配额采集结果
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from enum import Enum


class QuotaStatus(Enum):
    """配额采集状态"""
    SUCCESS = "success"    # 成功获取配额
    SKIPPED = "skipped"    # 跳过采集（有明确原因）
    FAILED = "failed"      # 采集失败


@dataclass
class QuotaResult:
    """配额采集结果"""
    service: str                    # 服务代码
    quota_code: str                 # 配额代码
    quota_name: str                 # 配额名称
    status: QuotaStatus              # 采集状态
    account_id: str = "default"     # 账号 ID
    region: str = "us-east-1"       # 区域
    reason: Optional[str] = None    # 状态原因（skipped 或 failed 时必须有）
    quota_info: Optional[Dict[str, Any]] = None  # 配额信息（success 时包含 limit_value 等）
    error: Optional[str] = None      # 错误信息（failed 时）
    
    def is_success(self) -> bool:
        """判断是否成功"""
        return self.status == QuotaStatus.SUCCESS
    
    def is_skipped(self) -> bool:
        """判断是否跳过"""
        return self.status == QuotaStatus.SKIPPED
    
    def is_failed(self) -> bool:
        """判断是否失败"""
        return self.status == QuotaStatus.FAILED

