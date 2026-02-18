# -*- coding: utf-8 -*-
"""
EKS API 客户端模块

功能：
- 封装 EKS API 调用（ListClusters, ListNodegroups 等）
- 获取集群、节点组、Fargate profiles 等资源信息
"""

import boto3
import logging
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class EKSClient:
    """
    EKS API 客户端
    
    功能：
    - 调用 EKS API 获取资源信息
    - 返回标准化的资源数据
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 EKS 客户端
        
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
                self.client = session.client('eks', region_name=region)
                logger.debug(f"EKS 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                self.client = boto3.client('eks', region_name=region)
                logger.debug(f"EKS 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 EKS 客户端失败: {e}")
            raise
    
    def list_clusters(self) -> List[str]:
        """
        列出所有 EKS 集群
        
        Returns:
            集群名称列表
        """
        try:
            clusters = []
            
            paginator = self.client.get_paginator('list_clusters')
            
            for page in paginator.paginate():
                clusters.extend(page.get('clusters', []))
            
            logger.debug(f"获取到 {len(clusters)} 个 EKS 集群")
            return clusters
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"ListClusters 失败: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"ListClusters 失败: {e}")
            raise
    
    def list_nodegroups(self, cluster_name: str) -> List[str]:
        """
        列出指定集群的所有节点组
        
        Args:
            cluster_name: 集群名称
        
        Returns:
            节点组名称列表
        """
        try:
            nodegroups = []
            
            paginator = self.client.get_paginator('list_nodegroups')
            
            for page in paginator.paginate(clusterName=cluster_name):
                nodegroups.extend(page.get('nodegroups', []))
            
            logger.debug(f"集群 {cluster_name} 有 {len(nodegroups)} 个节点组")
            return nodegroups
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"ListNodegroups 失败 (cluster: {cluster_name}): {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"ListNodegroups 失败 (cluster: {cluster_name}): {e}")
            raise
    
    def list_fargate_profiles(self, cluster_name: str) -> List[str]:
        """
        列出指定集群的所有 Fargate profiles
        
        Args:
            cluster_name: 集群名称
        
        Returns:
            Fargate profile 名称列表
        """
        try:
            profiles = []
            
            paginator = self.client.get_paginator('list_fargate_profiles')
            
            for page in paginator.paginate(clusterName=cluster_name):
                profiles.extend(page.get('fargateProfileNames', []))
            
            logger.debug(f"集群 {cluster_name} 有 {len(profiles)} 个 Fargate profiles")
            return profiles
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"ListFargateProfiles 失败 (cluster: {cluster_name}): {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"ListFargateProfiles 失败 (cluster: {cluster_name}): {e}")
            raise
    
    def describe_nodegroup(self, cluster_name: str, nodegroup_name: str) -> Dict[str, Any]:
        """
        描述指定节点组的详细信息
        
        Args:
            cluster_name: 集群名称
            nodegroup_name: 节点组名称
        
        Returns:
            节点组详细信息字典，包含 scalingConfig.desiredSize 等字段
        """
        try:
            response = self.client.describe_nodegroup(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name
            )
            
            nodegroup = response.get('nodegroup', {})
            logger.debug(f"获取节点组 {cluster_name}/{nodegroup_name} 详情成功")
            return nodegroup
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"DescribeNodegroup 失败 (cluster: {cluster_name}, nodegroup: {nodegroup_name}): {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"DescribeNodegroup 失败 (cluster: {cluster_name}, nodegroup: {nodegroup_name}): {e}")
            raise

