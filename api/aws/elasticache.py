# -*- coding: utf-8 -*-
"""
ElastiCache API 客户端模块

功能：
- 封装 ElastiCache API 调用（DescribeCacheClusters, DescribeReplicationGroups 等）
- 获取缓存集群、节点等资源信息
"""

import boto3
import logging
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class ElastiCacheClient:
    """
    ElastiCache API 客户端
    
    功能：
    - 调用 ElastiCache API 获取资源信息
    - 返回标准化的资源数据
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 ElastiCache 客户端
        
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
                self.client = session.client('elasticache', region_name=region)
                logger.debug(f"ElastiCache 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                self.client = boto3.client('elasticache', region_name=region)
                logger.debug(f"ElastiCache 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 ElastiCache 客户端失败: {e}")
            raise
    
    def describe_cache_clusters(self, show_cache_node_info: bool = True) -> List[Dict[str, Any]]:
        """
        描述所有缓存集群
        
        Args:
            show_cache_node_info: 是否包含节点详细信息（默认 True）
        
        Returns:
            缓存集群列表，每个包含 CacheClusterId, Engine, NumCacheNodes 等字段
        """
        try:
            clusters = []
            
            paginator = self.client.get_paginator('describe_cache_clusters')
            
            for page in paginator.paginate(ShowCacheNodeInfo=show_cache_node_info):
                for cluster in page.get('CacheClusters', []):
                    clusters.append({
                        'CacheClusterId': cluster.get('CacheClusterId', ''),
                        'Engine': cluster.get('Engine', ''),
                        'EngineVersion': cluster.get('EngineVersion', ''),
                        'NumCacheNodes': cluster.get('NumCacheNodes', 0),
                        'CacheNodeType': cluster.get('CacheNodeType', ''),
                        'ReplicationGroupId': cluster.get('ReplicationGroupId', ''),
                        'CacheClusterStatus': cluster.get('CacheClusterStatus', '')
                    })
            
            logger.debug(f"获取到 {len(clusters)} 个缓存集群")
            return clusters
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"DescribeCacheClusters 失败: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"DescribeCacheClusters 失败: {e}")
            raise
    
    def describe_replication_groups(self) -> List[Dict[str, Any]]:
        """
        描述所有复制组（Redis 集群模式）
        
        Returns:
            复制组列表，每个包含 ReplicationGroupId, Status, NodeGroups 等字段
        """
        try:
            replication_groups = []
            
            paginator = self.client.get_paginator('describe_replication_groups')
            
            for page in paginator.paginate():
                for rg in page.get('ReplicationGroups', []):
                    # 计算每个 NodeGroup 的节点数
                    node_groups = rg.get('NodeGroups', [])
                    nodes_per_nodegroup = []
                    for ng in node_groups:
                        node_group_members = ng.get('NodeGroupMembers', [])
                        nodes_per_nodegroup.append(len(node_group_members))
                    
                    replication_groups.append({
                        'ReplicationGroupId': rg.get('ReplicationGroupId', ''),
                        'Status': rg.get('Status', ''),
                        'NodeGroups': node_groups,
                        'NodesPerNodeGroup': nodes_per_nodegroup,  # 每个节点组的节点数列表
                        'TotalNodes': sum(nodes_per_nodegroup) if nodes_per_nodegroup else 0  # 总节点数
                    })
            
            logger.debug(f"获取到 {len(replication_groups)} 个复制组")
            return replication_groups
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"DescribeReplicationGroups 失败: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"DescribeReplicationGroups 失败: {e}")
            raise
    
    def describe_serverless_caches(self) -> List[Dict[str, Any]]:
        """
        描述所有 Serverless 缓存
        
        Returns:
            Serverless 缓存列表，每个包含 ServerlessCacheName, Status 等字段
        """
        try:
            serverless_caches = []
            
            paginator = self.client.get_paginator('describe_serverless_caches')
            
            for page in paginator.paginate():
                for cache in page.get('ServerlessCaches', []):
                    serverless_caches.append({
                        'ServerlessCacheName': cache.get('ServerlessCacheName', ''),
                        'Status': cache.get('Status', ''),
                        'Engine': cache.get('Engine', '')
                    })
            
            logger.debug(f"获取到 {len(serverless_caches)} 个 Serverless 缓存")
            return serverless_caches
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"DescribeServerlessCaches 失败: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"DescribeServerlessCaches 失败: {e}")
            raise

