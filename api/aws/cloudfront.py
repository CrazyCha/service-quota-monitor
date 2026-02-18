# -*- coding: utf-8 -*-
"""
CloudFront API 客户端模块

功能：
- 封装 CloudFront API 调用（ListDistributions 等）
- 获取分配信息
"""

import logging
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class CloudFrontClient:
    """
    CloudFront API 客户端
    
    功能：
    - 调用 CloudFront API 获取资源信息
    - 返回标准化的资源数据
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 CloudFront 客户端
        
        Args:
            region: AWS Region（CloudFront 是全局服务，固定使用 us-east-1）
            access_key: AWS Access Key（可选）
            secret_key: AWS Secret Key（可选）
        """
        self.region = region
        
        try:
            if access_key and secret_key:
                # 使用指定的凭证创建 Session
                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
                self.client = session.client('cloudfront', region_name=region)
            else:
                # 使用默认凭证链
                self.client = boto3.client('cloudfront', region_name=region)
            
            logger.debug(f"CloudFront 客户端初始化成功 (region: {region})")
        except Exception as e:
            logger.error(f"CloudFront 客户端初始化失败: {e}")
            raise
    
    def list_distributions(self) -> List[Dict[str, Any]]:
        """
        获取所有 CloudFront Distributions 列表（支持分页）
        
        Returns:
            Distribution 列表，每个 Distribution 是一个字典
        """
        try:
            logger.debug(f"调用 CloudFront ListDistributions API (region: {self.region})")
            
            all_distributions = []
            marker = None
            
            while True:
                # 构建请求参数
                request_params = {}
                if marker:
                    request_params['Marker'] = marker
                
                # 调用 API
                response = self.client.list_distributions(**request_params)
                
                # 提取 Distributions
                distribution_list = response.get('DistributionList', {})
                items = distribution_list.get('Items', [])
                all_distributions.extend(items)
                
                logger.debug(f"获取到 {len(items)} 个 Distributions (累计: {len(all_distributions)})")
                
                # 检查是否有下一页
                if distribution_list.get('IsTruncated', False):
                    marker = distribution_list.get('NextMarker')
                    if not marker:
                        logger.warning("CloudFront ListDistributions 返回 IsTruncated=True 但 NextMarker 为空")
                        break
                else:
                    break
            
            logger.info(f"CloudFront ListDistributions 完成，共 {len(all_distributions)} 个 Distributions")
            return all_distributions
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"CloudFront ListDistributions 失败: {error_code} - {error_message}")
            raise
        except BotoCoreError as e:
            logger.error(f"CloudFront ListDistributions 失败（BotoCoreError）: {e}")
            raise
        except Exception as e:
            logger.error(f"CloudFront ListDistributions 失败（未知错误）: {e}", exc_info=True)
            raise

