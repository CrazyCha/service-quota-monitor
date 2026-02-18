# -*- coding: utf-8 -*-
"""
EC2/EBS API 客户端模块

功能：
- 封装 EC2 API 调用（DescribeVolumes, DescribeSnapshots, DescribeAddresses 等）
- 返回资源数据供使用量计算
"""

import boto3
import logging
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class EC2Client:
    """
    EC2 API 客户端
    
    功能：
    - 调用 EC2 Describe API 获取资源信息
    - 支持过滤和分页
    - 返回标准化的资源数据
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 EC2 客户端
        
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
                self.client = session.client('ec2', region_name=region)
                logger.debug(f"EC2 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                self.client = boto3.client('ec2', region_name=region)
                logger.debug(f"EC2 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 EC2 客户端失败: {e}")
            raise
    
    def describe_volumes(self, volume_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        描述 EBS 卷
        
        Args:
            volume_type: 卷类型过滤（如 'gp3', 'io1', 'io2', 'st1', 'sc1'）
        
        Returns:
            卷列表，每个卷包含 Size, VolumeType, Iops 等字段
        """
        try:
            volumes = []
            filters = []
            
            if volume_type:
                filters.append({'Name': 'volume-type', 'Values': [volume_type]})
            
            paginator = self.client.get_paginator('describe_volumes')
            
            for page in paginator.paginate(Filters=filters if filters else None):
                for volume in page.get('Volumes', []):
                    volumes.append({
                        'VolumeId': volume.get('VolumeId', ''),
                        'Size': volume.get('Size', 0),  # GiB
                        'VolumeType': volume.get('VolumeType', ''),
                        'Iops': volume.get('Iops', 0),
                        'State': volume.get('State', '')
                    })
            
            logger.debug(f"获取到 {len(volumes)} 个卷 (type: {volume_type or 'all'})")
            return volumes
            
        except Exception as e:
            logger.error(f"DescribeVolumes 失败: {e}")
            raise
    
    def describe_snapshots(self, owner_id: str = 'self') -> List[Dict[str, Any]]:
        """
        描述 EBS 快照
        
        Args:
            owner_id: 所有者 ID（默认 'self'）
        
        Returns:
            快照列表
        """
        try:
            snapshots = []
            
            paginator = self.client.get_paginator('describe_snapshots')
            
            for page in paginator.paginate(OwnerIds=[owner_id]):
                for snapshot in page.get('Snapshots', []):
                    snapshots.append({
                        'SnapshotId': snapshot.get('SnapshotId', ''),
                        'VolumeId': snapshot.get('VolumeId', ''),
                        'State': snapshot.get('State', ''),
                        'StartTime': snapshot.get('StartTime')
                    })
            
            logger.debug(f"获取到 {len(snapshots)} 个快照")
            return snapshots
            
        except Exception as e:
            logger.error(f"DescribeSnapshots 失败: {e}")
            raise
    
    def describe_addresses(self) -> List[Dict[str, Any]]:
        """
        描述弹性 IP 地址
        
        Returns:
            弹性 IP 列表
        """
        try:
            addresses = []
            
            response = self.client.describe_addresses()
            
            for address in response.get('Addresses', []):
                addresses.append({
                    'AllocationId': address.get('AllocationId', ''),
                    'PublicIp': address.get('PublicIp', ''),
                    'Domain': address.get('Domain', ''),
                    'AssociationId': address.get('AssociationId')
                })
            
            logger.debug(f"获取到 {len(addresses)} 个弹性 IP")
            return addresses
            
        except Exception as e:
            logger.error(f"DescribeAddresses 失败: {e}")
            raise
    
    def describe_vpn_connections(self) -> List[Dict[str, Any]]:
        """
        描述 VPN 连接
        
        Returns:
            VPN 连接列表
        """
        try:
            vpn_connections = []
            
            response = self.client.describe_vpn_connections()
            
            for vpn in response.get('VpnConnections', []):
                vpn_connections.append({
                    'VpnConnectionId': vpn.get('VpnConnectionId', ''),
                    'State': vpn.get('State', ''),
                    'Type': vpn.get('Type', '')
                })
            
            logger.debug(f"获取到 {len(vpn_connections)} 个 VPN 连接")
            return vpn_connections
            
        except Exception as e:
            logger.error(f"DescribeVpnConnections 失败: {e}")
            raise
    
    def describe_instances(self, filters: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
        """
        描述 EC2 实例
        
        Args:
            filters: 过滤条件列表（如 [{'Name': 'instance-state-name', 'Values': ['running']}]
        
        Returns:
            实例列表，每个包含 InstanceId, InstanceType, InstanceLifecycle, State 等字段
        """
        try:
            instances = []
            
            paginator = self.client.get_paginator('describe_instances')
            
            for page in paginator.paginate(Filters=filters if filters else None):
                for reservation in page.get('Reservations', []):
                    for instance in reservation.get('Instances', []):
                        instances.append({
                            'InstanceId': instance.get('InstanceId', ''),
                            'InstanceType': instance.get('InstanceType', ''),
                            'InstanceLifecycle': instance.get('InstanceLifecycle', 'normal'),  # 'normal' 或 'spot'
                            'State': instance.get('State', {}).get('Name', ''),
                            'CpuOptions': instance.get('CpuOptions', {})
                        })
            
            logger.debug(f"获取到 {len(instances)} 个实例")
            return instances
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            logger.error(f"DescribeInstances 失败: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"DescribeInstances 失败: {e}")
            raise
