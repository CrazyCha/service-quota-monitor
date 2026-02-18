#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Service Quota Exporter 主程序入口

功能：
- 启动 Flask HTTP 服务器
- 暴露 /metrics 端点供 Prometheus 抓取
- 暴露 /health 健康检查端点
"""

from flask import Flask, jsonify
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import logging
import sys
import os
import urllib.request
import json
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# 导入配额配置加载模块
from config.loader import load_quota_config, print_quota_config, QuotaItem

# 导入 AWS Service Quotas 客户端
from provider.aws.service_quotas import ServiceQuotasClient

# 导入 Route53 API 客户端（用于直接获取配额）
from api.aws.route53 import Route53Client

# 导入 SageMaker Discovery
from provider.aws.sagemaker_discovery import SageMakerDiscovery

# 导入配额收集器
from collector import QuotaCollector, QuotaResult, QuotaStatus

# 导入 Provider Discovery
from provider.discovery import (
    AccountProvider, RegionProvider,
    CMDBAccountProvider, CMDBRegionProvider
)
from provider.discovery.credential_provider import (
    CredentialProvider,
    CMDBCredentialProvider
)
from provider.discovery.active_region_discoverer import ActiveRegionDiscoverer

# 导入 Usage Collector
from provider.aws.usage_collector import EC2UsageCollector, EBSUsageCollector, ELBUsageCollector, EKSUsageCollector, ElastiCacheUsageCollector, Route53UsageCollector, CloudFrontUsageCollector, SageMakerUsageCollector
from cache.cache import MemoryCache
from cache.quota_limit_cache import QuotaLimitCache

# 导入 Scheduler
from scheduler.scheduler import QuotaScheduler

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,  # 使用 DEBUG 级别以显示详细日志
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 设置特定模块的日志级别
logging.getLogger('werkzeug').setLevel(logging.WARNING)  # 减少 Flask 日志

# 创建 Flask 应用
app = Flask(__name__)

# 全局配额收集器（在 main 函数中初始化）
quota_collector = None


@app.route('/metrics')
def metrics():
    """
    Prometheus metrics 端点
    
    返回所有配额相关的 Prometheus 指标
    格式：Prometheus text format
    """
    if quota_collector is None:
        # 如果收集器未初始化，返回空指标
        return "# Exporter not initialized\n", 200, {'Content-Type': CONTENT_TYPE_LATEST}
    
    # 返回 Prometheus 指标
    metrics_data = quota_collector.get_metrics()
    return metrics_data, 200, {'Content-Type': CONTENT_TYPE_LATEST}


@app.route('/health')
def health():
    """
    健康检查端点
    
    返回 exporter 的健康状态
    """
    status = {'status': 'healthy'}
    
    # 如果 scheduler 已启动，添加定时任务状态
    global scheduler
    if scheduler:
        status['scheduler'] = scheduler.get_status()
    
    return status, 200


@app.route('/trigger/sagemaker/limit', methods=['POST'])
def trigger_sagemaker_limit():
    """
    手动触发 SageMaker Limit 采集
    
    返回 JSON 格式的结果
    """
    global _quota_config, _account_provider, _region_provider, _credential_provider, _quota_collector, _usage_collectors
    
    if not all([_quota_config, _account_provider, _region_provider, _quota_collector]):
        return jsonify({
            'success': False,
            'error': 'Exporter 未初始化，无法执行采集'
        }), 503
    
    try:
        logger.info("[手动触发] 开始 SageMaker Limit 采集...")
        
        collect_quotas(
            quota_config=_quota_config,
            account_provider=_account_provider,
            region_provider=_region_provider,
            quota_collector=_quota_collector,
            usage_collectors=_usage_collectors or {},
            credential_provider=_credential_provider,
            collect_limit=True,
            collect_usage=False,
            quota_limit_cache=_quota_limit_cache
        )
        
        # 统计 SageMaker Limit 指标数量
        try:
            with urllib.request.urlopen('http://localhost:8000/metrics', timeout=5) as response:
                metrics_text = response.read().decode('utf-8')
            sagemaker_limit_count = len([line for line in metrics_text.split('\n') 
                                         if 'service="sagemaker"' in line and 'cloud_service_quota_limit' in line])
        except Exception as e:
            logger.warning(f"[手动触发] 无法统计指标数量: {e}")
            sagemaker_limit_count = 0
        
        return jsonify({
            'success': True,
            'message': 'SageMaker Limit 采集完成',
            'sagemaker_limit_count': sagemaker_limit_count
        }), 200
        
    except Exception as e:
        logger.error(f"[手动触发] SageMaker Limit 采集失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/trigger/sagemaker/usage', methods=['POST'])
def trigger_sagemaker_usage():
    """
    手动触发 SageMaker Usage 采集
    
    返回 JSON 格式的结果
    """
    global _quota_config, _account_provider, _region_provider, _credential_provider, _quota_collector, _usage_collectors
    
    if not all([_quota_config, _account_provider, _region_provider, _quota_collector, _usage_collectors]):
        return jsonify({
            'success': False,
            'error': 'Exporter 未初始化，无法执行采集'
        }), 503
    
    try:
        logger.info("[手动触发] 开始 SageMaker Usage 采集...")
        
        collect_quotas(
            quota_config=_quota_config,
            account_provider=_account_provider,
            region_provider=_region_provider,
            quota_collector=_quota_collector,
            usage_collectors=_usage_collectors,
            credential_provider=_credential_provider,
            collect_limit=False,
            collect_usage=True,
            quota_limit_cache=_quota_limit_cache
        )
        
        # 统计 SageMaker Usage 指标数量（非 NaN）
        try:
            with urllib.request.urlopen('http://localhost:8000/metrics', timeout=5) as response:
                metrics_text = response.read().decode('utf-8')
            sagemaker_usage_count = len([line for line in metrics_text.split('\n') 
                                         if 'service="sagemaker"' in line 
                                         and 'cloud_service_quota_usage' in line 
                                         and 'NaN' not in line])
        except Exception as e:
            logger.warning(f"[手动触发] 无法统计指标数量: {e}")
            sagemaker_usage_count = 0
        
        return jsonify({
            'success': True,
            'message': 'SageMaker Usage 采集完成',
            'sagemaker_usage_count': sagemaker_usage_count
        }), 200
        
    except Exception as e:
        logger.error(f"[手动触发] SageMaker Usage 采集失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/trigger/sagemaker/all', methods=['POST'])
def trigger_sagemaker_all():
    """
    手动触发 SageMaker Limit 和 Usage 采集
    
    返回 JSON 格式的结果
    """
    global _quota_config, _account_provider, _region_provider, _credential_provider, _quota_collector, _usage_collectors, _quota_limit_cache
    
    if not all([_quota_config, _account_provider, _region_provider, _quota_collector, _usage_collectors]):
        return jsonify({
            'success': False,
            'error': 'Exporter 未初始化，无法执行采集'
        }), 503
    
    try:
        logger.info("[手动触发] 开始 SageMaker Limit 和 Usage 采集...")
        
        # 先采集 Limit
        logger.info("[手动触发] 步骤 1/2: 采集 SageMaker Limit...")
        collect_quotas(
            quota_config=_quota_config,
            account_provider=_account_provider,
            region_provider=_region_provider,
            quota_collector=_quota_collector,
            usage_collectors=_usage_collectors,
            credential_provider=_credential_provider,
            collect_limit=True,
            collect_usage=False,
            quota_limit_cache=_quota_limit_cache
        )
        
        # 再采集 Usage
        logger.info("[手动触发] 步骤 2/2: 采集 SageMaker Usage...")
        collect_quotas(
            quota_config=_quota_config,
            account_provider=_account_provider,
            region_provider=_region_provider,
            quota_collector=_quota_collector,
            usage_collectors=_usage_collectors,
            credential_provider=_credential_provider,
            collect_limit=False,
            collect_usage=True,
            quota_limit_cache=_quota_limit_cache
        )
        
        # 统计指标数量
        try:
            with urllib.request.urlopen('http://localhost:8000/metrics', timeout=5) as response:
                metrics_text = response.read().decode('utf-8')
            sagemaker_limit_count = len([line for line in metrics_text.split('\n') 
                                         if 'service="sagemaker"' in line and 'cloud_service_quota_limit' in line])
            sagemaker_usage_count = len([line for line in metrics_text.split('\n') 
                                         if 'service="sagemaker"' in line 
                                         and 'cloud_service_quota_usage' in line 
                                         and 'NaN' not in line])
        except Exception as e:
            logger.warning(f"[手动触发] 无法统计指标数量: {e}")
            sagemaker_limit_count = 0
            sagemaker_usage_count = 0
        
        return jsonify({
            'success': True,
            'message': 'SageMaker Limit 和 Usage 采集完成',
            'sagemaker_limit_count': sagemaker_limit_count,
            'sagemaker_usage_count': sagemaker_usage_count
        }), 200
        
    except Exception as e:
        logger.error(f"[手动触发] SageMaker 采集失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# 全局变量（在 main 函数中初始化）
scheduler: Optional[QuotaScheduler] = None
_quota_config: Optional[Any] = None
_account_provider: Optional[AccountProvider] = None
_region_provider: Optional[RegionProvider] = None
_credential_provider: Optional[CredentialProvider] = None
_quota_collector: Optional[QuotaCollector] = None
_usage_collectors: Optional[Dict[str, Any]] = None
_quota_limit_cache: Optional[QuotaLimitCache] = None


def collect_limit():
    """
    采集 Limit 数据
    
    供 Scheduler 调用，不关心账号、region、service 细节
    """
    global _quota_config, _account_provider, _region_provider, _credential_provider, _quota_collector, _usage_collectors, _quota_limit_cache
    
    if not all([_quota_config, _account_provider, _region_provider, _quota_collector, _usage_collectors]):
        logger.error("[Scheduler] 采集组件未初始化，无法执行 Limit 采集")
        return
    
    collect_quotas(
        quota_config=_quota_config,
        account_provider=_account_provider,
        region_provider=_region_provider,
        quota_collector=_quota_collector,
        usage_collectors=_usage_collectors,
        credential_provider=_credential_provider,
        collect_limit=True,
        collect_usage=False,
        quota_limit_cache=_quota_limit_cache
    )


def collect_usage():
    """
    采集 Usage 数据
    
    供 Scheduler 调用，不关心账号、region、service 细节
    """
    global _quota_config, _account_provider, _region_provider, _credential_provider, _quota_collector, _usage_collectors, _quota_limit_cache
    
    if not all([_quota_config, _account_provider, _region_provider, _quota_collector, _usage_collectors]):
        logger.error("[Scheduler] 采集组件未初始化，无法执行 Usage 采集")
        return
    
    collect_quotas(
        quota_config=_quota_config,
        account_provider=_account_provider,
        region_provider=_region_provider,
        quota_collector=_quota_collector,
        usage_collectors=_usage_collectors,
        credential_provider=_credential_provider,
        collect_limit=False,
        collect_usage=True,
        quota_limit_cache=_quota_limit_cache
    )


def _collect_account_quotas(
    account_id: str,
    quota_config: Any,
    region_provider: RegionProvider,
    usage_collectors: Dict[str, Any],
    credential_provider = None,
    collect_limit: bool = True,
    collect_usage: bool = True,
    quota_limit_cache: QuotaLimitCache = None
) -> List[QuotaResult]:
    """
    采集单个账号的配额数据（辅助函数，用于并发采集）
    
    Args:
        account_id: 账号 ID
        quota_config: 配额配置对象
        region_provider: 区域 Provider
        usage_collectors: Usage Collectors 字典
        credential_provider: 凭证 Provider
        collect_limit: 是否采集 Limit
        collect_usage: 是否采集 Usage
    
    Returns:
        该账号的采集结果列表
    """
    account_results: List[QuotaResult] = []
    
    try:
        regions = region_provider.get_regions(account_id)
        
        # 如果账号没有 EC2 Region，至少添加一个默认 Region 用于全局服务采集
        if not regions:
            logger.warning(f"[采集] 账号 {account_id} 没有 EC2 Region，将只采集全局服务（使用 us-east-1）")
            regions = ['us-east-1']
        
        # 获取账号凭证
        credentials = None
        if credential_provider:
            try:
                credentials = credential_provider.get_credentials(account_id)
            except Exception as e:
                logger.warning(f"[采集] 获取账号 {account_id} 的凭证失败: {e}，使用默认凭证链")
                credentials = None
        
        for region in regions:
            try:
                account_results.extend(_collect_account_region_quotas(
                    account_id=account_id,
                    region=region,
                    quota_config=quota_config,
                    usage_collectors=usage_collectors,
                    credentials=credentials,
                    collect_limit=collect_limit,
                    collect_usage=collect_usage,
                    quota_limit_cache=quota_limit_cache
                ))
            except Exception as e:
                logger.error(f"[采集] 处理账号 {account_id} 区域 {region} 时发生错误: {e}", exc_info=True)
                continue
                
    except Exception as e:
        logger.error(f"[采集] 处理账号 {account_id} 时发生错误: {e}", exc_info=True)
    
    return account_results


def _collect_account_region_quotas(
    account_id: str,
    region: str,
    quota_config: Any,
    usage_collectors: Dict[str, Any],
    credentials: Dict[str, str] = None,
    collect_limit: bool = True,
    collect_usage: bool = True,
    quota_limit_cache: QuotaLimitCache = None
) -> List[QuotaResult]:
    """
    采集单个账号在单个区域的配额数据（辅助函数）
    
    Args:
        account_id: 账号 ID
        region: 区域
        quota_config: 配额配置对象
        usage_collectors: Usage Collectors 字典
        credentials: 账号凭证
        collect_limit: 是否采集 Limit
        collect_usage: 是否采集 Usage
    
    Returns:
        该账号在该区域的采集结果列表
    """
    region_results: List[QuotaResult] = []
    
    logger.info(f"[采集] 处理账号: {account_id}, 区域: {region}")
    
    # 初始化 Service Quotas 客户端
    sq_client = None
    if collect_limit:
        if credentials:
            sq_client = ServiceQuotasClient(
                region=region,
                access_key=credentials.get('access_key'),
                secret_key=credentials.get('secret_key')
            )
        else:
            sq_client = ServiceQuotasClient(region=region)
    
    # 收集 usage 数据（service-level）
    if collect_usage:
        global_services = ['route53', 'cloudfront']
        regional_services = ['ec2', 'ebs', 'elasticloadbalancing', 'eks', 'elasticache', 'sagemaker']
        
        for service in ['ec2', 'ebs', 'elasticloadbalancing', 'eks', 'elasticache', 'route53', 'cloudfront', 'sagemaker']:
            if service in quota_config.aws and service in usage_collectors:
                try:
                    collector = usage_collectors[service]
                    
                    if service in global_services:
                        usage_region = 'us-east-1'
                        metrics_region = 'us-east-1'
                    else:
                        usage_region = region
                        metrics_region = region
                    
                    if credentials:
                        usage_data = collector.collect_usage(
                            account_id=account_id,
                            region=usage_region,
                            access_key=credentials.get('access_key'),
                            secret_key=credentials.get('secret_key')
                        )
                    else:
                        usage_data = collector.collect_usage(account_id=account_id, region=usage_region)
                    
                    if usage_data:
                        # 注意：这里不能直接调用 quota_collector，因为这是并发环境
                        # 需要返回 usage_data，由主函数统一设置
                        region_results.append({
                            'type': 'usage_data',
                            'account_id': account_id,
                            'region': metrics_region,
                            'service': service,
                            'usage_data': usage_data
                        })
                except Exception as e:
                    logger.error(f"[采集] 收集 {service} usage 失败: {e}", exc_info=True)
    
    # 采集 Limit 数据
    if collect_limit and sq_client:
        # 遍历所有服务的配额
        for service, service_config in quota_config.aws.items():
            # 全局服务列表（固定 Region）
            global_services = ['route53', 'cloudfront']
            # 区域型服务列表（只在 EC2 使用的 Region 采集）
            regional_services = ['ec2', 'ebs', 'elasticloadbalancing', 'eks', 'elasticache', 'sagemaker']
            
            # 确定该服务使用的 region
            # 全局服务固定 us-east-1，区域型服务使用当前 region（已经是 EC2 使用的 Region）
            if service in global_services:
                service_region = 'us-east-1'
            else:
                service_region = region
            
            # 如果 region 不匹配，需要重新初始化客户端（全局服务）
            if service in global_services and region != 'us-east-1':
                if credentials:
                    sq_client = ServiceQuotasClient(
                        region='us-east-1',
                        access_key=credentials.get('access_key'),
                        secret_key=credentials.get('secret_key')
                    )
                else:
                    sq_client = ServiceQuotasClient(region='us-east-1')
            
            # 处理 Discovery 模式（如 SageMaker）
            if isinstance(service_config, dict) and 'discovery' in service_config:
                discovery_config = service_config['discovery']
                if discovery_config.enabled:
                    logger.info(f"[采集] 服务 {service} 使用 Discovery 模式，区域: {service_region}")
                    
                    try:
                        # 初始化 Discovery（使用当前 region 的客户端）
                        # 对于区域型服务，需要确保使用正确的 region
                        if service in regional_services and service_region != sq_client.region:
                            # 重新初始化客户端到正确的 region
                            if credentials:
                                sq_client = ServiceQuotasClient(
                                    region=service_region,
                                    access_key=credentials.get('access_key'),
                                    secret_key=credentials.get('secret_key')
                                )
                            else:
                                sq_client = ServiceQuotasClient(region=service_region)
                        
                        discovery = SageMakerDiscovery(sq_client, discovery_config)
                        
                        # 发现匹配的配额
                        discovered_quotas = discovery.discover_quotas(service_region)
                        
                        if discovered_quotas:
                            logger.info(f"[采集] 服务 {service} 发现 {len(discovered_quotas)} 个匹配的配额")
                            
                            # 对每个发现的配额获取 Limit 值
                            for quota_item in discovered_quotas:
                                quota_code = quota_item.quota_code
                                quota_name = quota_item.quota_name
                                
                                try:
                                    # 添加延迟，避免 API 限流（增加到0.1秒，减少限流）
                                    time.sleep(0.1)
                                    
                                    # 调用 GetServiceQuota API 获取 Limit（带重试）
                                    max_retries = 3
                                    retry_count = 0
                                    quota_info = None
                                    
                                    while retry_count < max_retries:
                                        try:
                                            quota_info = sq_client.get_service_quota(
                                                service_code=service,
                                                quota_code=quota_code
                                            )
                                            break  # 成功，退出重试循环
                                        except Exception as api_error:
                                            error_msg = str(api_error)
                                            if 'TooManyRequestsException' in error_msg and retry_count < max_retries - 1:
                                                # API 限流，等待后重试（指数退避）
                                                wait_time = (2 ** retry_count) * 2  # 2, 4, 8 秒
                                                logger.warning(f"[采集] API 限流，等待 {wait_time} 秒后重试... (配额: {quota_code})")
                                                time.sleep(wait_time)
                                                retry_count += 1
                                            else:
                                                raise  # 其他错误或达到最大重试次数，抛出异常
                                    
                                    if quota_info:
                                        limit_value = quota_info.get('value', 0.0)
                                        
                                        # 创建成功结果（包含 account_id 和 region）
                                        quota_info_with_context = quota_info.copy()
                                        quota_info_with_context['account_id'] = account_id
                                        quota_info_with_context['region'] = service_region
                                        
                                        result = QuotaResult(
                                            service=service,
                                            quota_code=quota_code,
                                            quota_name=quota_name,
                                            status=QuotaStatus.SUCCESS,
                                            quota_info=quota_info_with_context,
                                            account_id=account_id,
                                            region=service_region
                                        )
                                        region_results.append(result)
                                        
                                        logger.debug(f"[采集] 配额 {quota_code}: Limit = {limit_value}")
                                    else:
                                        logger.warning(f"[采集] 配额 {quota_code}: 返回值为空")
                                        
                                        # 创建失败结果
                                        result = QuotaResult(
                                            service=service,
                                            quota_code=quota_code,
                                            quota_name=quota_name,
                                            status=QuotaStatus.FAILED,
                                            reason='empty_response',
                                            error='API returned empty response',
                                            account_id=account_id,
                                            region=service_region
                                        )
                                        region_results.append(result)
                                        
                                except Exception as e:
                                    error_msg = str(e)
                                    logger.error(f"[采集] 配额 {quota_code}: {error_msg}")
                                    
                                    # 创建失败结果
                                    reason = 'api_error'
                                    if 'NoSuchResourceException' in error_msg:
                                        reason = 'quota_not_found'
                                    elif 'AccessDeniedException' in error_msg:
                                        reason = 'permission_denied'
                                    
                                    result = QuotaResult(
                                        service=service,
                                        quota_code=quota_code,
                                        quota_name=quota_name,
                                        status=QuotaStatus.FAILED,
                                        reason=reason,
                                        error=error_msg,
                                        account_id=account_id,
                                        region=service_region
                                    )
                                    region_results.append(result)
                        else:
                            logger.warning(f"[采集] 服务 {service} 未发现匹配的配额")
                            
                    except Exception as e:
                        logger.error(f"[采集] 服务 {service} Discovery 失败: {e}", exc_info=True)
                        # Discovery 失败不影响其他服务，继续处理下一个服务
                    
                    # Discovery 模式处理完成，继续下一个服务
                    continue
            
            # CloudFront 特殊处理：配额不在 Service Quotas API 中，使用硬编码的默认值
            if service == 'cloudfront':
                logger.info(f"[采集] 服务 {service} 使用硬编码 Limit 值（配额不在 Service Quotas API 中）")
                
                # CloudFront 的默认配额限制值（AWS 标准默认值）
                cloudfront_default_limits = {
                    'L-24B04930': 200.0,  # Web distributions per AWS account
                    'L-7D134442': 20.0,   # Cache policies per AWS account
                    'L-CF0D4FC5': 20.0,   # Response headers policies
                    'L-08884E5C': 100.0   # Origin access identities per account
                }
                
                # 处理声明型配额
                quotas = service_config
                if not isinstance(quotas, list):
                    continue
                
                # 为每个 CloudFront 配额创建带有硬编码 Limit 值的结果
                for quota in quotas:
                    quota_code = quota.quota_code
                    quota_name = quota.quota_name
                    
                    # 获取硬编码的默认值
                    default_limit = cloudfront_default_limits.get(quota_code)
                    
                    if default_limit is not None:
                        # 创建成功结果（使用硬编码的 Limit 值）
                        quota_info_with_context = {
                            'quota_code': quota_code,
                            'quota_name': quota_name,
                            'value': default_limit,
                            'account_id': account_id,
                            'region': service_region
                        }
                        
                        result = QuotaResult(
                            service=service,
                            quota_code=quota_code,
                            quota_name=quota_name,
                            status=QuotaStatus.SUCCESS,
                            quota_info=quota_info_with_context,
                            account_id=account_id,
                            region=service_region
                        )
                        region_results.append(result)
                        
                        logger.debug(f"[采集] CloudFront 配额 {quota_code}: Limit = {default_limit} (硬编码默认值)")
                    else:
                        logger.warning(f"[采集] CloudFront 配额 {quota_code}: 未找到默认值，跳过")
                
                # CloudFront 处理完成，继续下一个服务
                continue
            
            # 处理声明型配额
            quotas = service_config
            if not isinstance(quotas, list):
                continue
            
            logger.debug(f"[采集] 服务: {service}, 配额数量: {len(quotas)}, region: {service_region}")
            
            for quota in quotas:
                quota_code = quota.quota_code
                quota_name = quota.quota_name
                
                try:
                    # 先检查缓存（如果启用）
                    quota_info = None
                    cache_hit = False
                    
                    if quota_limit_cache and not quota_limit_cache.is_force_refresh():
                        cached_data = quota_limit_cache.get(account_id, service_region, service, quota_code)
                        if cached_data:
                            quota_info = cached_data
                            cache_hit = True
                            logger.debug(f"[采集] 使用缓存的配额 Limit: {account_id}:{service_region}:{service}:{quota_code}")
                    
                    # 如果缓存未命中，调用 API
                    if not quota_info:
                        # 添加延迟，避免 API 限流（增加到0.1秒，减少限流）
                        time.sleep(0.1)
                        
                        # 调用 GetServiceQuota API（带重试）
                        max_retries = 3
                        retry_count = 0
                        
                        while retry_count < max_retries:
                            try:
                                quota_info = sq_client.get_service_quota(
                                    service_code=service,
                                    quota_code=quota_code
                                )
                                break  # 成功，退出重试循环
                            except Exception as api_error:
                                error_msg = str(api_error)
                                if 'TooManyRequestsException' in error_msg and retry_count < max_retries - 1:
                                    # API 限流，等待后重试（指数退避）
                                    wait_time = (2 ** retry_count) * 2  # 2, 4, 8 秒
                                    logger.warning(f"[采集] API 限流，等待 {wait_time} 秒后重试... (配额: {quota_code})")
                                    time.sleep(wait_time)
                                    retry_count += 1
                                else:
                                    raise  # 其他错误或达到最大重试次数，抛出异常
                        
                        # 如果 API 调用成功，更新缓存
                        if quota_info and quota_limit_cache:
                            quota_limit_cache.set(account_id, service_region, service, quota_code, quota_info)
                    
                    if quota_info:
                        limit_value = quota_info.get('value', 0.0)
                        
                        # 创建成功结果（包含 account_id 和 region）
                        quota_info_with_context = quota_info.copy()
                        quota_info_with_context['account_id'] = account_id
                        quota_info_with_context['region'] = service_region
                        
                        result = QuotaResult(
                            service=service,
                            quota_code=quota_code,
                            quota_name=quota_name,
                            status=QuotaStatus.SUCCESS,
                            quota_info=quota_info_with_context,
                            account_id=account_id,
                            region=service_region
                        )
                        region_results.append(result)
                        
                        logger.debug(f"[采集] 配额 {quota_code}: Limit = {limit_value}")
                    else:
                        logger.warning(f"[采集] 配额 {quota_code}: 返回值为空")
                        
                        # 创建失败结果
                        result = QuotaResult(
                            service=service,
                            quota_code=quota_code,
                            quota_name=quota_name,
                            status=QuotaStatus.FAILED,
                            reason='empty_response',
                            error='API returned empty response',
                            account_id=account_id,
                            region=service_region
                        )
                        region_results.append(result)
                        
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"[采集] 配额 {quota_code}: {error_msg}")
                    
                    # 创建失败结果
                    reason = 'api_error'
                    if 'NoSuchResourceException' in error_msg:
                        reason = 'quota_not_found'
                    elif 'AccessDeniedException' in error_msg:
                        reason = 'permission_denied'
                    
                    result = QuotaResult(
                        service=service,
                        quota_code=quota_code,
                        quota_name=quota_name,
                        status=QuotaStatus.FAILED,
                        reason=reason,
                        error=error_msg,
                        account_id=account_id,
                        region=service_region
                    )
                    region_results.append(result)
    
    return region_results


def collect_quotas(
    quota_config: Any,
    account_provider: AccountProvider,
    region_provider: RegionProvider,
    quota_collector: QuotaCollector,
    usage_collectors: Dict[str, Any],
    credential_provider = None,
    collect_limit: bool = True,
    collect_usage: bool = True,
    quota_limit_cache: QuotaLimitCache = None
) -> None:
    """
    采集配额数据的核心函数（支持并发采集）
    
    功能：
    - 根据 collect_limit 和 collect_usage 参数决定采集哪些数据
    - 使用并发采集提升性能
    - 可以被定时任务调用，也可以被主流程调用
    
    Args:
        quota_config: 配额配置对象
        account_provider: 账号 Provider
        region_provider: 区域 Provider
        quota_collector: 配额收集器
        usage_collectors: Usage Collectors 字典
        credential_provider: 凭证 Provider
        collect_limit: 是否采集 Limit（默认 True）
        collect_usage: 是否采集 Usage（默认 True）
    """
    # 获取账号和区域列表
    accounts = account_provider.get_accounts()
    
    # 存储所有采集结果
    all_results: List[QuotaResult] = []
    
    # 并发采集配置
    max_workers = int(os.getenv('COLLECTION_MAX_WORKERS', '3'))  # 默认 3 个并发线程（减少限流）
    use_concurrent = os.getenv('USE_CONCURRENT_COLLECTION', 'true').lower() == 'true'
    
    if use_concurrent and len(accounts) > 1:
        logger.info(f"[采集] 使用并发采集模式（{max_workers} 个并发线程）")
        results_lock = Lock()
        
        def collect_account_wrapper(account_id: str):
            """包装函数，用于并发采集"""
            try:
                return _collect_account_quotas(
                    account_id=account_id,
                    quota_config=quota_config,
                    region_provider=region_provider,
                    usage_collectors=usage_collectors,
                    credential_provider=credential_provider,
                    collect_limit=collect_limit,
                    collect_usage=collect_usage,
                    quota_limit_cache=quota_limit_cache
                )
            except Exception as e:
                logger.error(f"[采集] 账号 {account_id} 采集失败: {e}", exc_info=True)
                return []
        
        # 使用线程池并发采集
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_account = {executor.submit(collect_account_wrapper, account_id): account_id 
                                for account_id in accounts}
            
            completed = 0
            usage_data_list = []  # 存储所有账号的 Usage 数据
            
            for future in as_completed(future_to_account):
                account_id = future_to_account[future]
                completed += 1
                try:
                    account_results = future.result()
                    
                    # 分离 Limit 结果和 Usage 数据
                    limit_results = []
                    for item in account_results:
                        if isinstance(item, dict) and item.get('type') == 'usage_data':
                            # 这是 Usage 数据，需要单独处理
                            usage_data_list.append(item)
                        elif isinstance(item, QuotaResult):
                            # 这是 Limit 结果
                            limit_results.append(item)
                    
                    with results_lock:
                        all_results.extend(limit_results)
                    
                    logger.info(f"[采集] 账号 {account_id} 采集完成 ({completed}/{len(accounts)})")
                except Exception as e:
                    logger.error(f"[采集] 账号 {account_id} 采集异常: {e}", exc_info=True)
            
            # 统一设置所有账号的 Usage 数据
            if collect_usage and usage_data_list:
                logger.info(f"[采集] 开始设置 Usage 数据（共 {len(usage_data_list)} 个服务）")
                for usage_item in usage_data_list:
                    try:
                        quota_collector.set_usage_data(
                            account_id=usage_item['account_id'],
                            region=usage_item['region'],
                            service=usage_item['service'],
                            usage_data=usage_item['usage_data']
                        )
                    except Exception as e:
                        logger.error(f"[采集] 设置 Usage 数据失败: {e}", exc_info=True)
    else:
        # 顺序采集（原有逻辑）
        logger.info(f"[采集] 使用顺序采集模式")
        for account_id in accounts:
            regions = region_provider.get_regions(account_id)
        
        # 如果账号没有 EC2 Region，至少添加一个默认 Region 用于全局服务采集
        # 全局服务（Route53, CloudFront）使用 us-east-1
        if not regions:
            logger.warning(f"[采集] 账号 {account_id} 没有 EC2 Region，将只采集全局服务（使用 us-east-1）")
            regions = ['us-east-1']  # 至少有一个 Region 用于全局服务采集
        
        # 获取账号凭证（如果 credential_provider 存在）
        credentials = None
        if credential_provider:
            try:
                credentials = credential_provider.get_credentials(account_id)
                if credentials:
                    logger.debug(f"[采集] 账号 {account_id} 使用指定凭证")
                else:
                    logger.debug(f"[采集] 账号 {account_id} 使用默认凭证链")
            except Exception as e:
                logger.warning(f"[采集] 获取账号 {account_id} 的凭证失败: {e}，使用默认凭证链")
                credentials = None
        
        for region in regions:
            logger.info(f"\n[采集] 处理账号: {account_id}, 区域: {region}")
            
            try:
                # 初始化 Service Quotas 客户端（Limit 采集需要）
                # 注意：区域型服务使用当前 region，全局服务使用 us-east-1
                sq_client = None
                if collect_limit:
                    # 对于区域型服务，region 已经是 EC2 使用的 Region
                    # 对于全局服务，使用 us-east-1
                    # 这里先初始化，后续根据服务类型选择 region
                    logger.info(f"[采集] 初始化 AWS Service Quotas 客户端 (region: {region})...")
                    if credentials:
                        sq_client = ServiceQuotasClient(
                            region=region,
                            access_key=credentials.get('access_key'),
                            secret_key=credentials.get('secret_key')
                        )
                    else:
                        sq_client = ServiceQuotasClient(region=region)
                    logger.info("[采集] 客户端初始化成功")
                
                # 收集 usage 数据（service-level）
                if collect_usage:
                    # 全局服务列表（固定 Region）
                    global_services = ['route53', 'cloudfront']
                    # 区域型服务列表（只在 EC2 使用的 Region 采集）
                    regional_services = ['ec2', 'ebs', 'elasticloadbalancing', 'eks', 'elasticache', 'sagemaker']
                    
                    for service in ['ec2', 'ebs', 'elasticloadbalancing', 'eks', 'elasticache', 'route53', 'cloudfront', 'sagemaker']:
                        if service in quota_config.aws and service in usage_collectors:
                            try:
                                logger.info(f"[采集] 收集 {service} usage 数据...")
                                collector = usage_collectors[service]
                                
                                # 全局服务固定 Region（Route53/CloudFront → us-east-1）
                                if service in global_services:
                                    usage_region = 'us-east-1'
                                    metrics_region = 'us-east-1'
                                # 区域型服务只在 EC2 使用的 Region 采集（region 已经是 EC2 使用的 Region）
                                else:
                                    usage_region = region
                                    metrics_region = region
                                
                                # 传递凭证给 collector（如果存在）
                                if credentials:
                                    usage_data = collector.collect_usage(
                                        account_id=account_id, 
                                        region=usage_region,
                                        access_key=credentials.get('access_key'),
                                        secret_key=credentials.get('secret_key')
                                    )
                                else:
                                    usage_data = collector.collect_usage(account_id=account_id, region=usage_region)
                                
                                if usage_data:
                                    
                                    quota_collector.set_usage_data(
                                        account_id=account_id,
                                        region=metrics_region,
                                        service=service,
                                        usage_data=usage_data
                                    )
                                    logger.info(f"[采集] {service} usage 数据已设置: {len(usage_data)} 个配额")
                                else:
                                    logger.warning(f"[采集] {service} usage 数据为空")
                            except Exception as e:
                                logger.error(f"[采集] 收集 {service} usage 失败: {e}", exc_info=True)
                
                # 采集 Limit 数据
                if collect_limit and sq_client:
                    # 遍历所有服务的配额
                    for service, service_config in quota_config.aws.items():
                        # 全局服务列表（固定 Region）
                        global_services = ['route53', 'cloudfront']
                        # 区域型服务列表（只在 EC2 使用的 Region 采集）
                        regional_services = ['ec2', 'ebs', 'elasticloadbalancing', 'eks', 'elasticache', 'sagemaker']
                        
                        # 确定该服务使用的 region
                        # 全局服务固定 us-east-1，区域型服务使用当前 region（已经是 EC2 使用的 Region）
                        if service in global_services:
                            service_region = 'us-east-1'
                        else:
                            service_region = region
                        
                        # 如果 region 不匹配，需要重新初始化客户端（全局服务）
                        if service in global_services and region != 'us-east-1':
                            if credentials:
                                sq_client = ServiceQuotasClient(
                                    region='us-east-1',
                                    access_key=credentials.get('access_key'),
                                    secret_key=credentials.get('secret_key')
                                )
                            else:
                                sq_client = ServiceQuotasClient(region='us-east-1')
                        
                        # 处理 Discovery 模式（如 SageMaker）
                        if isinstance(service_config, dict) and 'discovery' in service_config:
                            discovery_config = service_config['discovery']
                            if discovery_config.enabled:
                                logger.info(f"[采集] 服务 {service} 使用 Discovery 模式，区域: {service_region}")
                                
                                try:
                                    # 初始化 Discovery（使用当前 region 的客户端）
                                    # 对于区域型服务，需要确保使用正确的 region
                                    if service in regional_services and service_region != sq_client.region:
                                        # 重新初始化客户端到正确的 region
                                        if credentials:
                                            sq_client = ServiceQuotasClient(
                                                region=service_region,
                                                access_key=credentials.get('access_key'),
                                                secret_key=credentials.get('secret_key')
                                            )
                                        else:
                                            sq_client = ServiceQuotasClient(region=service_region)
                                    
                                    discovery = SageMakerDiscovery(sq_client, discovery_config)
                                    
                                    # 发现匹配的配额
                                    discovered_quotas = discovery.discover_quotas(service_region)
                                    
                                    if discovered_quotas:
                                        logger.info(f"[采集] 服务 {service} 发现 {len(discovered_quotas)} 个匹配的配额")
                                        
                                        # 对每个发现的配额获取 Limit 值
                                        for quota_item in discovered_quotas:
                                            quota_code = quota_item.quota_code
                                            quota_name = quota_item.quota_name
                                            
                                            try:
                                                # 添加延迟，避免 API 限流（增加到0.1秒，减少限流）
                                                time.sleep(0.1)
                                                
                                                # 调用 GetServiceQuota API 获取 Limit（带重试）
                                                max_retries = 3
                                                retry_count = 0
                                                quota_info = None
                                                
                                                while retry_count < max_retries:
                                                    try:
                                                        quota_info = sq_client.get_service_quota(
                                                            service_code=service,
                                                            quota_code=quota_code
                                                        )
                                                        break  # 成功，退出重试循环
                                                    except Exception as api_error:
                                                        error_msg = str(api_error)
                                                        if 'TooManyRequestsException' in error_msg and retry_count < max_retries - 1:
                                                            # API 限流，等待后重试（指数退避）
                                                            wait_time = (2 ** retry_count) * 2  # 2, 4, 8 秒
                                                            logger.warning(f"[采集] API 限流，等待 {wait_time} 秒后重试... (配额: {quota_code})")
                                                            time.sleep(wait_time)
                                                            retry_count += 1
                                                        else:
                                                            raise  # 其他错误或达到最大重试次数，抛出异常
                                                
                                                if quota_info:
                                                    limit_value = quota_info.get('value', 0.0)
                                                    
                                                    # 创建成功结果（包含 account_id 和 region）
                                                    quota_info_with_context = quota_info.copy()
                                                    quota_info_with_context['account_id'] = account_id
                                                    quota_info_with_context['region'] = service_region
                                                    
                                                    result = QuotaResult(
                                                        service=service,
                                                        quota_code=quota_code,
                                                        quota_name=quota_name,
                                                        status=QuotaStatus.SUCCESS,
                                                        quota_info=quota_info_with_context,
                                                        account_id=account_id,
                                                        region=service_region
                                                    )
                                                    all_results.append(result)
                                                    
                                                    logger.debug(f"[采集] 配额 {quota_code}: Limit = {limit_value}")
                                                else:
                                                    logger.warning(f"[采集] 配额 {quota_code}: 返回值为空")
                                                    
                                                    # 创建失败结果
                                                    result = QuotaResult(
                                                        service=service,
                                                        quota_code=quota_code,
                                                        quota_name=quota_name,
                                                        status=QuotaStatus.FAILED,
                                                        reason='empty_response',
                                                        error='API returned empty response',
                                                        account_id=account_id,
                                                        region=service_region
                                                    )
                                                    all_results.append(result)
                                                    
                                            except Exception as e:
                                                error_msg = str(e)
                                                logger.error(f"[采集] 配额 {quota_code}: {error_msg}")
                                                
                                                # 创建失败结果
                                                reason = 'api_error'
                                                if 'NoSuchResourceException' in error_msg:
                                                    reason = 'quota_not_found'
                                                elif 'AccessDeniedException' in error_msg:
                                                    reason = 'permission_denied'
                                                
                                                result = QuotaResult(
                                                    service=service,
                                                    quota_code=quota_code,
                                                    quota_name=quota_name,
                                                    status=QuotaStatus.FAILED,
                                                    reason=reason,
                                                    error=error_msg,
                                                    account_id=account_id,
                                                    region=service_region
                                                )
                                                all_results.append(result)
                                    else:
                                        logger.warning(f"[采集] 服务 {service} 未发现匹配的配额")
                                        
                                except Exception as e:
                                    logger.error(f"[采集] 服务 {service} Discovery 失败: {e}", exc_info=True)
                                    # Discovery 失败不影响其他服务，继续处理下一个服务
                                
                                # Discovery 模式处理完成，继续下一个服务
                                continue
                        
                        # CloudFront 特殊处理：配额不在 Service Quotas API 中，使用硬编码的默认值
                        if service == 'cloudfront':
                            logger.info(f"[采集] 服务 {service} 使用硬编码 Limit 值（配额不在 Service Quotas API 中）")
                            
                            # CloudFront 的默认配额限制值（AWS 标准默认值）
                            cloudfront_default_limits = {
                                'L-24B04930': 200.0,  # Web distributions per AWS account
                                'L-7D134442': 20.0,   # Cache policies per AWS account
                                'L-CF0D4FC5': 20.0,   # Response headers policies
                                'L-08884E5C': 100.0   # Origin access identities per account
                            }
                            
                            # 处理声明型配额
                            quotas = service_config
                            if not isinstance(quotas, list):
                                continue
                            
                            # 为每个 CloudFront 配额创建带有硬编码 Limit 值的结果
                            for quota in quotas:
                                quota_code = quota.quota_code
                                quota_name = quota.quota_name
                                
                                # 获取硬编码的默认值
                                default_limit = cloudfront_default_limits.get(quota_code)
                                
                                if default_limit is not None:
                                    # 创建成功结果（使用硬编码的 Limit 值）
                                    quota_info_with_context = {
                                        'quota_code': quota_code,
                                        'quota_name': quota_name,
                                        'value': default_limit,
                                        'account_id': account_id,
                                        'region': service_region
                                    }
                                    
                                    result = QuotaResult(
                                        service=service,
                                        quota_code=quota_code,
                                        quota_name=quota_name,
                                        status=QuotaStatus.SUCCESS,
                                        quota_info=quota_info_with_context,
                                        account_id=account_id,
                                        region=service_region
                                    )
                                    all_results.append(result)
                                    
                                    logger.debug(f"[采集] CloudFront 配额 {quota_code}: Limit = {default_limit} (硬编码默认值)")
                                else:
                                    logger.warning(f"[采集] CloudFront 配额 {quota_code}: 未找到默认值，跳过")
                            
                            # CloudFront 处理完成，继续下一个服务
                            continue
                        
                        # Route53 特殊处理已移除
                        # L-4EA4796A (Hosted zones per account) 和 L-F767CB15 (Domain count limit) 都在 Service Quotas API 中
                        # 应该通过正常的 Service Quotas API 流程获取
                        
                        # 处理声明型配额
                        quotas = service_config
                        if not isinstance(quotas, list):
                            continue
                        
                        logger.debug(f"[采集] 服务: {service}, 配额数量: {len(quotas)}, region: {service_region}")
                        
                        for quota in quotas:
                            quota_code = quota.quota_code
                            quota_name = quota.quota_name
                            
                            try:
                                # 添加延迟，避免 API 限流（增加到0.1秒，减少限流）
                                time.sleep(0.1)
                                
                                # 调用 GetServiceQuota API（带重试）
                                max_retries = 3
                                retry_count = 0
                                quota_info = None
                                
                                while retry_count < max_retries:
                                    try:
                                        quota_info = sq_client.get_service_quota(
                                            service_code=service,
                                            quota_code=quota_code
                                        )
                                        break  # 成功，退出重试循环
                                    except Exception as api_error:
                                        error_msg = str(api_error)
                                        if 'TooManyRequestsException' in error_msg and retry_count < max_retries - 1:
                                            # API 限流，等待后重试（指数退避）
                                            wait_time = (2 ** retry_count) * 2  # 2, 4, 8 秒
                                            logger.warning(f"[采集] API 限流，等待 {wait_time} 秒后重试... (配额: {quota_code})")
                                            time.sleep(wait_time)
                                            retry_count += 1
                                        else:
                                            raise  # 其他错误或达到最大重试次数，抛出异常
                                
                                if quota_info:
                                    limit_value = quota_info.get('value', 0.0)
                                    
                                    # 创建成功结果（包含 account_id 和 region）
                                    quota_info_with_context = quota_info.copy()
                                    quota_info_with_context['account_id'] = account_id
                                    quota_info_with_context['region'] = service_region  # 使用正确的 region
                                    
                                    result = QuotaResult(
                                        service=service,
                                        quota_code=quota_code,
                                        quota_name=quota_name,
                                        status=QuotaStatus.SUCCESS,
                                        quota_info=quota_info_with_context,
                                        account_id=account_id,
                                        region=service_region  # 使用正确的 region
                                    )
                                    all_results.append(result)
                                    
                                    logger.debug(f"[采集] 配额 {quota_code}: Limit = {limit_value}")
                                else:
                                    logger.warning(f"[采集] 配额 {quota_code}: 返回值为空")
                                    
                                    # 创建失败结果
                                    result = QuotaResult(
                                        service=service,
                                        quota_code=quota_code,
                                        quota_name=quota_name,
                                        status=QuotaStatus.FAILED,
                                        reason='empty_response',
                                        error='API returned empty response',
                                        account_id=account_id,
                                        region=region
                                    )
                                    all_results.append(result)
                                    
                            except Exception as e:
                                error_msg = str(e)
                                logger.error(f"[采集] 配额 {quota_code}: {error_msg}")
                                
                                # 创建失败结果
                                reason = 'api_error'
                                if 'NoSuchResourceException' in error_msg:
                                    reason = 'quota_not_found'
                                elif 'AccessDeniedException' in error_msg:
                                    reason = 'permission_denied'
                                
                                result = QuotaResult(
                                    service=service,
                                    quota_code=quota_code,
                                    quota_name=quota_name,
                                    status=QuotaStatus.FAILED,
                                    reason=reason,
                                    error=error_msg,
                                    account_id=account_id,
                                    region=region
                                )
                                all_results.append(result)
                
            except Exception as e:
                logger.error(f"[采集] 处理账号 {account_id} 区域 {region} 时发生错误: {e}", exc_info=True)
                continue
    
    # 将所有结果添加到收集器
    if all_results:
        logger.debug(f"[采集] 添加 {len(all_results)} 个采集结果到收集器")
        quota_collector.collect_all(all_results)
    
    # 在 Limit 采集之后，再次为 CloudFront 设置 Usage 数据
    # 因为 CloudFront 的 skipped 结果是在 Limit 采集阶段创建的，此时才能正确设置 Usage 指标
    # 使用已经采集并存储的 usage_data，而不是重新采集
    has_cloudfront_config = hasattr(quota_config, 'aws') and 'cloudfront' in quota_config.aws
    has_cloudfront_collector = 'cloudfront' in usage_collectors
    logger.debug(f"[采集] CloudFront 重新设置检查: collect_usage={collect_usage}, has_config={has_cloudfront_config}, has_collector={has_cloudfront_collector}")
    if collect_usage and has_cloudfront_config and has_cloudfront_collector:
        logger.info(f"[采集] 开始重新设置 CloudFront Usage（使用已采集的数据），账号数量: {len(accounts)}")
        cloudfront_region = 'us-east-1'
        for account_id in accounts:
            try:
                # 从已存储的 usage_data 中获取 CloudFront 数据
                usage_key = (account_id, cloudfront_region, 'cloudfront')
                usage_data = quota_collector.usage_data.get(usage_key)
                
                if usage_data:
                    # 此时 skipped 结果已经创建并添加，可以正确设置 Usage 指标
                    quota_collector.set_usage_data(
                        account_id=account_id,
                        region=cloudfront_region,
                        service='cloudfront',
                        usage_data=usage_data
                    )
                    logger.info(f"[采集] CloudFront usage 数据已重新设置: {len(usage_data)} 个配额（账号: {account_id}，在 Limit 采集之后）")
                else:
                    logger.warning(f"[采集] 账号 {account_id} 的 CloudFront usage 数据未找到，可能尚未采集")
            except Exception as e:
                logger.error(f"[采集] 重新设置 CloudFront usage 失败（账号: {account_id}）: {e}", exc_info=True)
    
    # 获取汇总信息
    summary = quota_collector.get_summary()
    
    logger.info(f"[采集] 采集完成: 总计={summary['total']}, 成功={summary['success']}, 跳过={summary['skipped']}, 失败={summary['failed']}")


def main():
    """
    主函数：启动 Flask 服务器
    
    功能：
    1. 加载配额配置文件
    2. 初始化 Provider Discovery
    3. 拉取 AWS Service Quotas
    4. 启动 HTTP 服务器
    """
    logger.info("Starting Service Quota Exporter...")
    
    # Phase 1: 加载配额配置
    quotas_path = 'config/quotas.yaml'
    
    # 如果 config/quotas.yaml 不存在，尝试使用根目录的 quotas.yaml
    if not os.path.exists(quotas_path):
        quotas_path = 'quotas.yaml'
        if not os.path.exists(quotas_path):
            logger.error(f"配额配置文件不存在: config/quotas.yaml 或 quotas.yaml")
            sys.exit(1)
    
    try:
        logger.info(f"正在加载配额配置: {quotas_path}")
        quota_config = load_quota_config(quotas_path)
        logger.info("配额配置加载成功")
        
        # 打印配置结构（Phase 1 验证）
        print_quota_config(quota_config)
        
        # 检查是否有 SageMaker discovery 配置
        if 'sagemaker' in quota_config.aws:
            sagemaker_config = quota_config.aws['sagemaker']
            if isinstance(sagemaker_config, dict) and 'discovery' in sagemaker_config:
                logger.info("=" * 60)
                logger.info("检测到 SageMaker Discovery 配置")
                logger.info("=" * 60)
                discovery = sagemaker_config['discovery']
                logger.info(f"SageMaker Discovery 配置详情:")
                logger.info(f"  - enabled: {discovery.enabled}")
                logger.info(f"  - match_rules 数量: {len(discovery.match_rules)}")
                for idx, rule in enumerate(discovery.match_rules):
                    keywords = rule.get('name_contains', [])
                    logger.info(f"  - 规则 {idx+1}: name_contains = {keywords}")
                logger.info(f"  - default_priority: {discovery.default_priority}")
                logger.info("=" * 60)
                logger.info("SageMaker Discovery 已启用，将在采集时动态发现配额")
                logger.info("=" * 60)
        
    except FileNotFoundError as e:
        logger.error(f"配置文件不存在: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"加载配额配置失败: {e}")
        sys.exit(1)
    
    # Phase 2: 初始化 Provider Discovery
    logger.info("=" * 60)
    logger.info("初始化 Provider Discovery")
    logger.info("=" * 60)
    
    # 使用 CMDB Provider（从 MySQL 数据库读取账号和区域）
    logger.info("使用 CMDB Provider")
    
    # 检查 DB_PASSWORD 是否设置
    if not os.getenv('DB_PASSWORD'):
        logger.error("=" * 60)
        logger.error("错误: DB_PASSWORD 环境变量未设置")
        logger.error("=" * 60)
        logger.error("CMDB 模式需要设置数据库密码，请运行:")
        logger.error("  export DB_PASSWORD='your_cmdb_password'")
        logger.error("=" * 60)
        sys.exit(1)
    
    account_provider: AccountProvider = CMDBAccountProvider()
    
    # 初始化 Credential Provider（从 CMDB 读取凭证）
    credential_provider: CredentialProvider = CMDBCredentialProvider(cmdb_account_provider=account_provider)
    
    # 初始化 EC2 Region 使用发现器
    logger.info("=" * 60)
    logger.info("初始化 EC2 Region 使用发现器")
    logger.info("=" * 60)
    logger.info("策略：")
    logger.info("  - Region 候选集从 CMDB 读取（视为静态输入）")
    logger.info("  - 仅在 CMDB 候选 Region 内探测 EC2 使用情况")
    logger.info("  - 只返回有 EC2 实例的 Region")
    logger.info("  - 结果按 account_id 缓存 24h")
    logger.info("=" * 60)
    
    region_discoverer = None
    try:
        region_discoverer = ActiveRegionDiscoverer()
        logger.info("EC2 Region 使用发现器初始化成功")
    except Exception as e:
        logger.error(f"EC2 Region 使用发现器初始化失败: {e}", exc_info=True)
        logger.warning("将使用默认 Region Provider")
    
    # 在启动时一次性发现所有账号的 EC2 Region（避免重复调用）
    active_regions_map = {}
    if region_discoverer:
        try:
            logger.info("=" * 60)
            logger.info("启动时发现所有账号的 EC2 Region（使用缓存）")
            logger.info("=" * 60)
            force_refresh = os.getenv('FORCE_REFRESH_EC2_REGIONS', 'false').lower() == 'true'
            active_regions_map = region_discoverer.discover_ec2_used_regions_from_provider(
                account_provider,
                use_cache=True,
                force_refresh=force_refresh
            )
            logger.info(f"发现完成：{len(active_regions_map)} 个账号有 EC2 Region 数据")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"启动时发现 EC2 Region 失败: {e}", exc_info=True)
            logger.warning("将使用默认 Region Provider")
    
    # 初始化 Region Provider（基于 EC2 使用发现，传入预发现的映射）
    region_provider: RegionProvider = CMDBRegionProvider(
        account_provider=account_provider,
        region_discoverer=region_discoverer,
        active_regions_map=active_regions_map
    )
    
    # 保存到全局变量（供 scheduler 使用）
    global _credential_provider
    _credential_provider = credential_provider
    
    # 获取账号和区域列表
    accounts = account_provider.get_accounts()
    all_regions = set()
    for account_id in accounts:
        regions = region_provider.get_regions(account_id)
        all_regions.update(regions)
    
    logger.info(f"Provider 类型: {account_provider.get_provider_type()}")
    logger.info(f"账号数量: {len(accounts)}")
    logger.info(f"账号列表: {accounts}")
    logger.info(f"区域数量: {len(all_regions)}")
    logger.info(f"区域列表: {sorted(all_regions)}")
    logger.info("=" * 60)
    
    # Phase 3: 初始化采集组件
    logger.info("=" * 60)
    logger.info("初始化采集组件")
    logger.info("=" * 60)
    
    # 初始化配额收集器
    global quota_collector
    quota_collector = QuotaCollector()
    
    # 初始化缓存（用于 usage 采集）
    usage_cache = MemoryCache()
    
    # 初始化配额 Limit 缓存（用于 Limit 采集优化）
    quota_limit_cache = QuotaLimitCache()
    logger.info(f"配额 Limit 缓存已启用: {quota_limit_cache.cache_dir}, TTL: {quota_limit_cache.cache_ttl} 秒")
    
    # 初始化 Usage Collectors（service-level）
    usage_collectors = {
        'ec2': EC2UsageCollector(cache=usage_cache),
        'ebs': EBSUsageCollector(cache=usage_cache),
        'elasticloadbalancing': ELBUsageCollector(cache=usage_cache),
        'eks': EKSUsageCollector(cache=usage_cache),
        'elasticache': ElastiCacheUsageCollector(cache=usage_cache),
        'route53': Route53UsageCollector(cache=usage_cache),
        'cloudfront': CloudFrontUsageCollector(cache=usage_cache),
        'sagemaker': SageMakerUsageCollector(cache=usage_cache)
    }
    
    # Phase 4: 执行初始采集
    logger.info("=" * 60)
    logger.info("执行初始采集")
    logger.info("=" * 60)
    
    try:
        # 执行初始采集（Limit + Usage）
        collect_quotas(
            quota_config=quota_config,
            account_provider=account_provider,
            region_provider=region_provider,
            quota_collector=quota_collector,
            usage_collectors=usage_collectors,
            credential_provider=credential_provider,
            collect_limit=True,
            collect_usage=True,
            quota_limit_cache=quota_limit_cache
        )
        
        # 获取汇总信息
        summary = quota_collector.get_summary()
        
        # 打印详细汇总
        print(f"\n{'=' * 60}")
        print("配额采集汇总")
        print(f"{'=' * 60}")
        print(f"总配额数: {summary['total']}")
        print(f"成功: {summary['success']}")
        print(f"跳过: {summary['skipped']}")
        print(f"失败: {summary['failed']}")
        
        if summary['by_service']:
            print(f"\n按服务统计:")
            for svc, stats in summary['by_service'].items():
                print(f"  {svc}: 成功={stats['success']}, 跳过={stats['skipped']}, 失败={stats['failed']}")
        
        if summary['skip_reasons']:
            print(f"\n跳过原因统计:")
            for reason, count in summary['skip_reasons'].items():
                print(f"  {reason}: {count}")
        
        print(f"{'=' * 60}")
        
        # 同时记录到日志
        logger.info(f"配额采集完成: 总计={summary['total']}, 成功={summary['success']}, 跳过={summary['skipped']}, 失败={summary['failed']}")
        
    except Exception as e:
        logger.error(f"初始采集失败: {e}", exc_info=True)
        logger.error("请检查:")
        logger.error("  1. AWS 凭证是否正确配置")
        logger.error("  2. 是否有 Service Quotas API 权限")
        logger.error("  3. 网络连接是否正常")
        # 不退出，继续启动服务器（用于测试）
    
    # Phase 5: 启动定时任务
    logger.info("=" * 60)
    logger.info("启动定时任务")
    logger.info("=" * 60)
    
    # 保存全局变量供 collect_limit() 和 collect_usage() 使用
    global _quota_config, _account_provider, _region_provider, _quota_collector, _usage_collectors, _quota_limit_cache
    _quota_config = quota_config
    _account_provider = account_provider
    _region_provider = region_provider
    _quota_collector = quota_collector
    _usage_collectors = usage_collectors
    _quota_limit_cache = quota_limit_cache
    
    # 初始化 Scheduler（只负责"什么时候刷新"）
    global scheduler
    scheduler = QuotaScheduler(
        collect_limit_func=collect_limit,
        collect_usage_func=collect_usage,
        limit_interval=86400,  # 24 小时
        usage_interval=3600     # 1 小时
    )
    
    # 启动定时任务（会在后台线程中运行）
    scheduler.start()
    
    logger.info("定时任务已启动，将在后台自动刷新数据")
    
    # 启动 Flask 服务器
    port = 8000  # 默认端口，可以从配置文件读取
    
    # 检查端口是否被占用
    import socket
    import subprocess
    
    def is_port_in_use(port):
        """检查端口是否被占用"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        return result == 0
    
    def try_kill_port(port):
        """尝试清理占用端口的进程"""
        try:
            # 查找占用端口的进程
            pid = subprocess.check_output(
                ['lsof', '-ti', f':{port}'],
                stderr=subprocess.DEVNULL
            ).decode('utf-8').strip()
            
            if pid:
                # 尝试停止进程
                subprocess.run(['kill', '-9', pid], 
                             stderr=subprocess.DEVNULL, 
                             stdout=subprocess.DEVNULL)
                logger.info(f"已清理占用端口 {port} 的进程 (PID: {pid})")
                # 等待一下让端口释放
                import time
                time.sleep(0.5)
                return True
        except:
            pass
        return False
    
    # 如果默认端口被占用，先尝试清理
    if is_port_in_use(port):
        logger.warning(f"端口 {port} 已被占用，尝试清理旧进程...")
        if try_kill_port(port):
            # 再次检查端口是否释放
            if is_port_in_use(port):
                logger.warning(f"端口 {port} 清理后仍被占用，使用端口 {port + 1}")
                port = port + 1
            else:
                logger.info(f"端口 {port} 已释放，使用默认端口")
        else:
            logger.warning(f"无法清理占用端口 {port} 的进程，使用端口 {port + 1}")
            port = port + 1
    
    logger.info(f"\nStarting HTTP server on port {port}")
    print(f"\n{'=' * 60}")
    print(f"Exporter 已启动")
    print(f"访问 http://localhost:{port}/metrics 查看指标")
    print(f"访问 http://localhost:{port}/health 查看健康状态")
    print(f"{'=' * 60}\n")
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == '__main__':
    main()
