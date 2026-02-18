# -*- coding: utf-8 -*-
"""
AWS Service Quotas API 客户端模块

功能：
- 封装 AWS Service Quotas API 调用
- 获取配额限制值
- 处理 SageMaker 配额模糊匹配
"""

import boto3
import logging
import time
from botocore.exceptions import ClientError, BotoCoreError
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ServiceQuotasClient:
    """
    AWS Service Quotas API 客户端
    
    功能：
    - 调用 ListServiceQuotas 获取服务配额列表
    - 调用 GetServiceQuota 获取特定配额详情
    - 处理 SageMaker 配额的模糊匹配逻辑
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 Service Quotas 客户端
        
        Args:
            region: AWS 区域（默认 us-east-1）
            access_key: AWS Access Key（可选，如果提供则使用指定凭证）
            secret_key: AWS Secret Key（可选，如果提供则使用指定凭证）
        """
        self.region = region
        try:
            # 如果提供了 access_key 和 secret_key，使用指定凭证
            if access_key and secret_key:
                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
                self.client = session.client('service-quotas', region_name=region)
                logger.debug(f"Service Quotas 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                # 使用默认凭证链（环境变量、配置文件、IAM 角色等）
                self.client = boto3.client('service-quotas', region_name=region)
                logger.debug(f"Service Quotas 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 Service Quotas 客户端失败: {e}")
            raise
    
    def get_service_quota(self, service_code: str, quota_code: str) -> Optional[Dict]:
        """
        获取特定配额的详细信息
        
        Args:
            service_code: 服务代码（如 'ec2'）
            quota_code: 配额代码（如 'L-1216C47A'）
        
        Returns:
            配额详情字典，包含：
            - quota_code: 配额代码
            - quota_name: 配额名称
            - value: 配额限制值
            - unit: 单位
        
        Raises:
            ClientError: AWS API 错误
        """
        try:
            logger.debug(f"调用 GetServiceQuota: service_code={service_code}, quota_code={quota_code}, region={self.region}")
            
            response = self.client.get_service_quota(
                ServiceCode=service_code,
                QuotaCode=quota_code
            )
            
            quota = response.get('Quota', {})
            result = {
                'quota_code': quota.get('QuotaCode', quota_code),
                'quota_name': quota.get('QuotaName', ''),
                'value': quota.get('Value', 0.0),
                'unit': quota.get('Unit', ''),
                'adjustable': quota.get('Adjustable', False),
                'global_quota': quota.get('GlobalQuota', False)
            }
            
            logger.debug(f"获取配额成功: {quota_code} = {result['value']} {result['unit']}")
            return result
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            if error_code == 'NoSuchResourceException':
                logger.warning(f"配额不存在: service_code={service_code}, quota_code={quota_code}, region={self.region}")
            elif error_code == 'AccessDeniedException':
                logger.error(f"权限不足: service_code={service_code}, quota_code={quota_code}, region={self.region}")
            elif error_code == 'TooManyRequestsException':
                # API 限流，需要等待后重试
                logger.warning(f"API 限流: service_code={service_code}, quota_code={quota_code}, region={self.region}，等待后重试...")
                # 等待 2 秒后重试（指数退避）
                time.sleep(2)
                # 重新抛出异常，让调用者处理重试
                raise
            else:
                logger.error(f"获取配额失败: service_code={service_code}, quota_code={quota_code}, region={self.region}, error={error_code}: {error_message}")
            
            raise
        except BotoCoreError as e:
            logger.error(f"AWS SDK 错误: service_code={service_code}, quota_code={quota_code}, region={self.region}, error={e}")
            raise
        except Exception as e:
            logger.error(f"未知错误: service_code={service_code}, quota_code={quota_code}, region={self.region}, error={e}")
            raise
    
    def list_service_quotas(self, service_code: str) -> list:
        """
        列出指定服务的所有配额
        
        Args:
            service_code: 服务代码（如 'ec2'）
        
        Returns:
            配额列表，每个配额包含 quota_code, quota_name, value 等字段
        
        TODO: 当前阶段未使用，保留接口供后续使用
        """
        quotas = []
        try:
            paginator = self.client.get_paginator('list_service_quotas')
            
            for page in paginator.paginate(ServiceCode=service_code):
                for quota in page.get('Quotas', []):
                    quotas.append({
                        'quota_code': quota.get('QuotaCode', ''),
                        'quota_name': quota.get('QuotaName', ''),
                        'value': quota.get('Value', 0.0),
                        'unit': quota.get('Unit', '')
                    })
            
            logger.debug(f"列出配额成功: service_code={service_code}, 共 {len(quotas)} 个配额")
            return quotas
            
        except Exception as e:
            logger.error(f"列出配额失败: service_code={service_code}, region={self.region}, error={e}")
            raise

