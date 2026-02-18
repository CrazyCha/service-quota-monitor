# -*- coding: utf-8 -*-
"""
AWS CloudWatch 指标收集模块

功能：
- 从 CloudWatch 获取配额使用量指标
- 构建指标查询请求
- 处理指标响应数据
"""

import boto3
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class CloudWatchClient:
    """
    CloudWatch 指标收集器
    
    功能：
    - 根据配额配置构建 CloudWatch 指标查询
    - 调用 CloudWatch API 获取指标值
    - 处理指标数据并返回使用量
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 CloudWatch 客户端
        
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
                self.client = session.client('cloudwatch', region_name=region)
                logger.debug(f"CloudWatch 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                self.client = boto3.client('cloudwatch', region_name=region)
                logger.debug(f"CloudWatch 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 CloudWatch 客户端失败: {e}")
            raise
    
    def get_metric_statistics(
        self,
        namespace: str,
        metric_name: str,
        dimensions: Dict[str, str],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
        statistic: str = 'Average'
    ) -> Optional[float]:
        """
        获取 CloudWatch 指标统计数据
        
        Args:
            namespace: 命名空间（如 'AWS/Usage'）
            metric_name: 指标名称
            dimensions: 维度字典
            start_time: 开始时间（默认：15分钟前）
            end_time: 结束时间（默认：现在）
            period: 统计周期（秒，默认 300）
            statistic: 统计方法（'Average', 'Sum', 'Maximum' 等）
        
        Returns:
            最新的指标值，如果无数据返回 None
        """
        try:
            if end_time is None:
                end_time = datetime.utcnow()
            if start_time is None:
                start_time = end_time - timedelta(minutes=15)
            
            # 构建维度列表
            dimension_list = [
                {'Name': k, 'Value': v}
                for k, v in dimensions.items()
            ]
            
            response = self.client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimension_list,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=[statistic]
            )
            
            datapoints = response.get('Datapoints', [])
            if not datapoints:
                # CloudWatch 无数据：这是正常行为，返回 None（上层会返回 NaN）
                logger.debug(f"CloudWatch 指标无数据: {namespace}/{metric_name} (dimensions: {dimensions})")
                return None
            
            # 返回最新的数据点值（按时间排序）
            datapoints.sort(key=lambda x: x['Timestamp'], reverse=True)
            latest_value = datapoints[0].get(statistic)
            
            logger.debug(f"CloudWatch 指标值: {namespace}/{metric_name} = {latest_value}")
            return latest_value
            
        except ClientError as e:
            # CloudWatch API 调用异常：重新抛出异常，让上层区分 API 异常和无数据
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"CloudWatch API 调用异常 {namespace}/{metric_name}: {error_code} - {error_message}")
            raise
        except Exception as e:
            # CloudWatch API 调用异常：重新抛出异常，让上层区分 API 异常和无数据
            logger.error(f"CloudWatch API 调用异常 {namespace}/{metric_name}: {e}")
            raise
