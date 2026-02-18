# -*- coding: utf-8 -*-
"""
Elastic Load Balancing API 客户端模块

功能：
- 封装 ELB API 调用（DescribeLoadBalancers, DescribeTargetGroups 等）
- 获取负载均衡器、目标组、规则等资源信息
"""

import boto3
import logging
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class ELBClient:
    """
    ELB API 客户端
    
    功能：
    - 调用 ELB Describe API 获取资源信息
    - 支持类型过滤（ALB/NLB）
    - 返回标准化的资源数据
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 ELB 客户端
        
        Args:
            region: AWS 区域
            access_key: AWS Access Key（可选，如果提供则使用指定凭证）
            secret_key: AWS Secret Key（可选，如果提供则使用指定凭证）
        """
        self.region = region
        try:
            if access_key and secret_key:
                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
                self.client = session.client('elbv2', region_name=region)
                logger.debug(f"ELB 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                # ELBv2 客户端（支持 ALB 和 NLB）
                self.client = boto3.client('elbv2', region_name=region)
                logger.debug(f"ELB 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 ELB 客户端失败: {e}")
            raise
    
    def describe_load_balancers(self, lb_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        描述负载均衡器
        
        Args:
            lb_type: 负载均衡器类型过滤（'application' 或 'network'）
        
        Returns:
            负载均衡器列表，每个包含 LoadBalancerArn, Type, State 等字段
        """
        try:
            load_balancers = []
            
            # 构建过滤条件
            filters = []
            if lb_type:
                filters.append({'Name': 'type', 'Values': [lb_type]})
            
            paginator = self.client.get_paginator('describe_load_balancers')
            
            for page in paginator.paginate():
                for lb in page.get('LoadBalancers', []):
                    # 如果指定了类型过滤，检查类型是否匹配
                    if lb_type and lb.get('Type', '').lower() != lb_type.lower():
                        continue
                    
                    load_balancers.append({
                        'LoadBalancerArn': lb.get('LoadBalancerArn', ''),
                        'LoadBalancerName': lb.get('LoadBalancerName', ''),
                        'Type': lb.get('Type', ''),
                        'State': lb.get('State', {}).get('Code', ''),
                        'Scheme': lb.get('Scheme', '')
                    })
            
            logger.debug(f"获取到 {len(load_balancers)} 个负载均衡器 (type: {lb_type or 'all'})")
            return load_balancers
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"DescribeLoadBalancers 失败: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"DescribeLoadBalancers 失败: {e}")
            raise
    
    def describe_target_groups(self) -> List[Dict[str, Any]]:
        """
        描述目标组
        
        Returns:
            目标组列表，每个包含 TargetGroupArn, TargetGroupName 等字段
        """
        try:
            target_groups = []
            
            paginator = self.client.get_paginator('describe_target_groups')
            
            for page in paginator.paginate():
                for tg in page.get('TargetGroups', []):
                    target_groups.append({
                        'TargetGroupArn': tg.get('TargetGroupArn', ''),
                        'TargetGroupName': tg.get('TargetGroupName', ''),
                        'Protocol': tg.get('Protocol', ''),
                        'Port': tg.get('Port', 0),
                        'HealthCheckProtocol': tg.get('HealthCheckProtocol', '')
                    })
            
            logger.debug(f"获取到 {len(target_groups)} 个目标组")
            return target_groups
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"DescribeTargetGroups 失败: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"DescribeTargetGroups 失败: {e}")
            raise

