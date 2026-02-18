# -*- coding: utf-8 -*-
"""
Prometheus Collector 实现模块

功能：
- 收集所有 Provider 的配额数据
- 更新 Prometheus 指标
- 提供指标数据供 /metrics 端点使用
"""

import time
import math
import logging
from typing import List, Dict, Optional
from prometheus_client import Gauge, Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from collector.quota_result import QuotaResult, QuotaStatus

logger = logging.getLogger(__name__)


class QuotaCollector:
    """
    配额收集器
    
    功能：
    - 管理配额采集结果
    - 更新 Prometheus 指标
    - 提供指标数据供 /metrics 端点使用
    """
    
    def __init__(self):
        """初始化配额收集器"""
        # 配额指标（冻结语义）
        # 1. cloud_service_quota_limit: 配额限制值
        self.quota_limit = Gauge(
            'cloud_service_quota_limit',
            'Cloud service quota limit value',
            ['provider', 'account_id', 'region', 'service', 'quota_name', 'quota_code']
        )
        
        # 2. cloud_service_quota_usage: 配额使用量（当前占位为 NaN）
        self.quota_usage = Gauge(
            'cloud_service_quota_usage',
            'Cloud service quota usage value',
            ['provider', 'account_id', 'region', 'service', 'quota_name', 'quota_code']
        )
        
        # 3. cloud_quota_usage_percent: 配额使用百分比（当前占位为 NaN）
        self.quota_usage_percent = Gauge(
            'cloud_quota_usage_percent',
            'Cloud service quota usage percentage (usage / limit * 100)',
            ['provider', 'account_id', 'region', 'service', 'quota_name', 'quota_code']
        )
        
        # Exporter 自身指标
        self.scrape_errors_total = Counter(
            'quota_exporter_scrape_errors_total',
            'Total number of scrape errors',
            ['service', 'quota_code', 'error_type']
        )
        
        self.scrape_duration_seconds = Histogram(
            'quota_exporter_scrape_duration_seconds',
            'Duration of quota collection in seconds',
            buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
        )
        
        self.scrape_skipped_total = Counter(
            'quota_exporter_scrape_skipped_total',
            'Total number of skipped quotas',
            ['service', 'reason']
        )
        
        # 存储采集结果
        self.results: List[QuotaResult] = []
        
        # 存储 usage 数据（service-level）
        # key: (account_id, region, service), value: {quota_code: usage_value}
        self.usage_data: Dict[tuple, Dict[str, float]] = {}
    
    def add_result(self, result: QuotaResult):
        """
        添加配额采集结果
        
        Args:
            result: 配额采集结果
        """
        self.results.append(result)
        
        # 根据状态更新指标
        if result.is_success():
            # 更新配额 limit 指标
            quota_info = result.quota_info or {}
            limit_value = quota_info.get('value', 0.0)
            
            # 从 result 中获取 account_id 和 region
            account_id = result.account_id
            region = result.region
            
            # 构建统一的 labels
            labels = {
                'provider': 'aws',
                'account_id': account_id,
                'region': region,
                'service': result.service,
                'quota_name': result.quota_name,
                'quota_code': result.quota_code
            }
            
            # 1. 设置 limit 值（从 API 获取）
            self.quota_limit.labels(**labels).set(limit_value)
            
            # 2. 设置 usage 值
            # 从 usage_data 中查找对应的 usage 值
            usage_value = self._get_usage_value(
                account_id=account_id,
                region=region,
                service=result.service,
                quota_code=result.quota_code
            )
            
            if usage_value is not None:
                # usage_value 可能是 0（账号没有使用资源），这是正常情况
                self.quota_usage.labels(**labels).set(usage_value)
                
                # 3. 设置 usage_percent 值
                # percent = (usage / limit) * 100
                if limit_value > 0:
                    percent_value = (usage_value / limit_value) * 100.0
                    self.quota_usage_percent.labels(**labels).set(percent_value)
                else:
                    self.quota_usage_percent.labels(**labels).set(float('nan'))
            else:
                # 没有 usage 数据，设置为 NaN（其他服务或未实现）
                self.quota_usage.labels(**labels).set(float('nan'))
                self.quota_usage_percent.labels(**labels).set(float('nan'))
            
        elif result.is_skipped():
            # 更新跳过计数
            self.scrape_skipped_total.labels(
                service=result.service,
                reason=result.reason or 'unknown'
            ).inc()
            
        elif result.is_failed():
            # 更新错误计数
            error_type = 'api_error'
            if 'NoSuchResourceException' in (result.error or ''):
                error_type = 'quota_not_found'
            elif 'AccessDeniedException' in (result.error or ''):
                error_type = 'permission_denied'
            
            self.scrape_errors_total.labels(
                service=result.service,
                quota_code=result.quota_code,
                error_type=error_type
            ).inc()
    
    def collect_all(self, results: List[QuotaResult]):
        """
        批量添加采集结果
        
        Args:
            results: 配额采集结果列表
        """
        start_time = time.time()
        
        for result in results:
            self.add_result(result)
        
        # 记录采集耗时
        duration = time.time() - start_time
        self.scrape_duration_seconds.observe(duration)
    
    def get_metrics(self) -> str:
        """
        获取 Prometheus 格式的指标数据
        
        Returns:
            Prometheus text format 字符串
        """
        return generate_latest().decode('utf-8')
    
    def set_usage_data(self, account_id: str, region: str, service: str, usage_data: Dict[str, float]):
        """
        设置服务的 usage 数据（service-level）
        
        Args:
            account_id: 账号 ID
            region: 区域
            service: 服务代码
            usage_data: {quota_code: usage_value} 字典
        """
        key = (account_id, region, service)
        self.usage_data[key] = usage_data
        
        # 更新已存在的指标（有 Limit 的情况）
        for result in self.results:
            if (result.is_success() and 
                result.quota_info and
                result.quota_info.get('account_id') == account_id and
                result.quota_info.get('region') == region and
                result.service == service):
                
                quota_code = result.quota_code
                if quota_code in usage_data:
                    usage_value = usage_data[quota_code]
                    limit_value = result.quota_info.get('value', 0.0)
                    
                    labels = {
                        'provider': 'aws',
                        'account_id': account_id,
                        'region': region,
                        'service': service,
                        'quota_name': result.quota_name,
                        'quota_code': quota_code
                    }
                    
                    # 更新 usage 指标
                    self.quota_usage.labels(**labels).set(usage_value)
                    
                    # 更新 percent 指标
                    if limit_value > 0:
                        percent_value = (usage_value / limit_value) * 100.0
                        self.quota_usage_percent.labels(**labels).set(percent_value)
                    else:
                        self.quota_usage_percent.labels(**labels).set(float('nan'))
        
        # 处理没有 Limit 的情况（如 CloudFront，配额不在 Service Quotas API 中）
        # 查找该服务的 skipped 结果，为它们设置 Usage
        for result in self.results:
            if (result.is_skipped() and
                result.account_id == account_id and
                result.region == region and
                result.service == service):
                
                quota_code = result.quota_code
                if quota_code in usage_data:
                    usage_value = usage_data[quota_code]
                    
                    labels = {
                        'provider': 'aws',
                        'account_id': account_id,
                        'region': region,
                        'service': service,
                        'quota_name': result.quota_name,
                        'quota_code': quota_code
                    }
                    
                    # 设置 usage 指标（即使没有 Limit）
                    self.quota_usage.labels(**labels).set(usage_value)
                    
                    # 没有 Limit，percent 设置为 NaN
                    self.quota_usage_percent.labels(**labels).set(float('nan'))
    
    def _get_usage_value(self, account_id: str, region: str, service: str, quota_code: str) -> Optional[float]:
        """
        获取 usage 值
        
        Args:
            account_id: 账号 ID
            region: 区域
            service: 服务代码
            quota_code: 配额代码
        
        Returns:
            usage 值，如果不存在返回 None
        """
        key = (account_id, region, service)
        usage_data = self.usage_data.get(key, {})
        value = usage_data.get(quota_code)
        
        # Debug 日志
        if service in ['ec2', 'ebs']:
            if not usage_data:
                logger.debug(f"Usage 数据为空: key={key}, service={service}")
            elif quota_code not in usage_data:
                logger.debug(f"配额 {quota_code} 不在 usage 数据中: available={list(usage_data.keys())}")
            else:
                logger.debug(f"找到 usage 值: {quota_code} = {value}")
        
        return value
    
    def get_summary(self) -> Dict:
        """
        获取采集汇总信息
        
        Returns:
            汇总信息字典
        """
        total = len(self.results)
        success = sum(1 for r in self.results if r.is_success())
        skipped = sum(1 for r in self.results if r.is_skipped())
        failed = sum(1 for r in self.results if r.is_failed())
        
        # 按服务统计
        by_service = {}
        for result in self.results:
            if result.service not in by_service:
                by_service[result.service] = {'success': 0, 'skipped': 0, 'failed': 0}
            if result.is_success():
                by_service[result.service]['success'] += 1
            elif result.is_skipped():
                by_service[result.service]['skipped'] += 1
            elif result.is_failed():
                by_service[result.service]['failed'] += 1
        
        # 按跳过原因统计
        skip_reasons = {}
        for result in self.results:
            if result.is_skipped() and result.reason:
                skip_reasons[result.reason] = skip_reasons.get(result.reason, 0) + 1
        
        return {
            'total': total,
            'success': success,
            'skipped': skipped,
            'failed': failed,
            'by_service': by_service,
            'skip_reasons': skip_reasons
        }
