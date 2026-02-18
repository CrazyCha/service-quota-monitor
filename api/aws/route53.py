# -*- coding: utf-8 -*-
"""
Route 53 API 客户端模块

功能：
- 封装 Route 53 API 调用（GetAccountLimit, ListHostedZones 等）
- 获取配额限制和资源使用量
"""

import boto3
import logging
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class Route53Client:
    """
    Route 53 API 客户端
    
    功能：
    - 调用 Route 53 API 获取配额限制
    - Route 53 是全局服务，不需要指定 region
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 Route 53 客户端
        
        Args:
            region: AWS 区域（Route 53 是全局服务，但 boto3 需要指定 region）
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
                self.client = session.client('route53', region_name=region)
                logger.debug(f"Route53 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                # Route 53 是全局服务，但 boto3 客户端需要指定 region（通常使用 us-east-1）
                # 使用默认凭证链（环境变量、配置文件、IAM 角色等）
                self.client = boto3.client('route53', region_name=region)
                logger.debug(f"Route53 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 Route53 客户端失败: {e}")
            raise
    
    def get_account_limit(self, limit_type: str) -> Optional[Dict[str, Any]]:
        """
        获取账户配额限制
        
        Args:
            limit_type: 限制类型，如 'MAX_HOSTED_ZONES', 'MAX_REUSABLE_DELEGATION_SETS' 等
        
        Returns:
            配额信息字典，包含 Limit 和 Count，如果失败返回 None
        """
        try:
            logger.debug(f"调用 Route53 GetAccountLimit API: limit_type={limit_type}, region={self.region}")
            response = self.client.get_account_limit(Type=limit_type)
            
            logger.debug(f"Route53 API 响应: {response}")
            
            # 检查响应结构
            if not response:
                logger.error(f"GetAccountLimit 返回空响应: limit_type={limit_type}")
                return None
            
            limit_info = response.get('Limit', {})
            count = response.get('Count', 0)
            
            # 检查 Limit 字段是否存在
            if not limit_info:
                logger.error(f"GetAccountLimit 响应中缺少 Limit 字段: limit_type={limit_type}, response={response}")
                return None
            
            # 检查 Value 字段是否存在
            value = limit_info.get('Value')
            if value is None:
                logger.error(f"GetAccountLimit Limit 中缺少 Value 字段: limit_type={limit_type}, limit_info={limit_info}")
                return None
            
            result = {
                'limit_type': limit_type,
                'value': value,
                'count': count
            }
            
            logger.info(f"获取 Route53 配额限制成功: {limit_type} = {result['value']}, 当前使用: {count}")
            return result
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"GetAccountLimit ClientError (type: {limit_type}): {error_code} - {error_message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"GetAccountLimit 异常 (type: {limit_type}): {e}", exc_info=True)
            return None
    
    def get_hosted_zones_limit(self) -> Optional[Dict[str, Any]]:
        """
        获取托管域名（Hosted Zones）配额限制
        
        Returns:
            配额信息字典，包含 limit 和 count
        """
        return self.get_account_limit('MAX_HOSTED_ZONES_BY_OWNER')
    
    def get_hosted_zone_count(self) -> Optional[int]:
        """
        获取托管区域（Hosted Zones）总数
        
        使用 GetHostedZoneCount API，这是获取 Hosted Zones 数量的推荐方法
        
        Returns:
            托管区域总数，如果失败返回 None
        """
        try:
            logger.debug(f"调用 Route53 GetHostedZoneCount API: region={self.region}")
            response = self.client.get_hosted_zone_count()
            
            logger.debug(f"Route53 GetHostedZoneCount API 响应: {response}")
            
            hosted_zone_count = response.get('HostedZoneCount', 0)
            logger.info(f"获取 Route53 Hosted Zones 总数成功: {hosted_zone_count}")
            return hosted_zone_count
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"GetHostedZoneCount ClientError: {error_code} - {error_message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"GetHostedZoneCount 异常: {e}", exc_info=True)
            return None
    
    def list_hosted_zones(self) -> int:
        """
        列出所有托管域名（用于 usage 采集）
        
        注意：推荐使用 get_hosted_zone_count() 方法，更高效
        
        Returns:
            托管域名数量
        """
        try:
            hosted_zones = []
            paginator = self.client.get_paginator('list_hosted_zones')
            
            for page in paginator.paginate():
                hosted_zones.extend(page.get('HostedZones', []))
            
            count = len(hosted_zones)
            logger.debug(f"获取到 {count} 个 Route53 托管域名")
            return count
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"ListHostedZones 失败: {error_code} - {error_message}")
            return 0
        except Exception as e:
            logger.error(f"ListHostedZones 失败: {e}")
            return 0

