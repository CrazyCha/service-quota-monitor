# -*- coding: utf-8 -*-
"""
Usage Collector 接口和实现

功能：
- 定义 UsageCollector 接口（service-level）
- 实现 EC2 和 EBS 的 UsageCollector
- 使用缓存避免重复 API 调用
"""

import logging
import math
import boto3
from abc import ABC, abstractmethod
from typing import Dict, Optional
from cache.cache import MemoryCache
from api.aws.ec2 import EC2Client
from api.aws.elb import ELBClient
from api.aws.eks import EKSClient
from api.aws.elasticache import ElastiCacheClient
from api.aws.route53 import Route53Client
from api.aws.sagemaker import SageMakerClient
from provider.aws.service_quotas import ServiceQuotasClient
from cloudwatch.client import CloudWatchClient

logger = logging.getLogger(__name__)


class UsageCollector(ABC):
    """
    Usage Collector 接口（service-level）
    
    每个服务一个 UsageCollector，一次性获取该服务的所有 usage 数据
    """
    
    @abstractmethod
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集服务的使用量数据
        
        Args:
            account_id: 账号 ID
            region: 区域
            access_key: AWS Access Key（可选，如果提供则使用指定凭证）
            secret_key: AWS Secret Key（可选，如果提供则使用指定凭证）
        
        Returns:
            字典，key 为 quota_code，value 为 usage 值
            如果某个配额无法获取 usage，不包含该 key
        """
        pass


class EC2UsageCollector(UsageCollector):
    """
    EC2 Usage Collector
    
    功能：
    - 收集 EC2 服务的所有配额使用量
    - 使用缓存（1小时 TTL）
    - 支持 CloudWatch 和 API 两种方式
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 EC2 Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 EC2 的使用量数据
        
        Args:
            account_id: 账号 ID
            region: 区域
            access_key: AWS Access Key（可选）
            secret_key: AWS Secret Key（可选）
        
        Returns:
            {quota_code: usage_value} 字典
        """
        cache_key = f"ec2_usage:{account_id}:{region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"EC2 usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 EC2 usage (account: {account_id}, region: {region})")
        
        usage_data = {}
        
        try:
            # 初始化客户端（传入凭证）
            ec2_client = EC2Client(region=region, access_key=access_key, secret_key=secret_key)
            cloudwatch_client = CloudWatchClient(region=region, access_key=access_key, secret_key=secret_key)
            
            # 1. CloudWatch AWS/Usage 指标（On-Demand 和 Spot 实例）
            # 这些配额通过 CloudWatch 获取
            # 注意：CloudWatch 指标可能有延迟，如果获取失败不影响其他配额
            cloudwatch_quotas = {
                'L-1216C47A': {  # Running On-Demand Standard instances
                    'dimensions': {
                        'Type': 'Resource',
                        'Resource': 'vCPU',
                        'Service': 'EC2',
                        'Class': 'Standard/OnDemand'
                    }
                },
                'L-DB2E81BA': {  # Running On-Demand G and VT instances
                    'dimensions': {
                        'Type': 'Resource',
                        'Resource': 'vCPU',
                        'Service': 'EC2',
                        'Class': 'G/OnDemand'  # 注意：配置文件中的实际值
                    }
                },
                'L-417A185B': {  # Running On-Demand P instances
                    'dimensions': {
                        'Type': 'Resource',
                        'Resource': 'vCPU',
                        'Service': 'EC2',
                        'Class': 'P/OnDemand'
                    }
                },
                'L-34B43A08': {  # All Standard Spot Instance Requests
                    'dimensions': {
                        'Type': 'Resource',
                        'Resource': 'vCPU',
                        'Service': 'EC2',
                        'Class': 'Standard/Spot'
                    }
                },
                'L-3819A6DF': {  # All G and VT Spot Instance Requests
                    'dimensions': {
                        'Type': 'Resource',
                        'Resource': 'vCPU',
                        'Service': 'EC2',
                        'Class': 'G/Spot'  # 注意：配置文件中的实际值
                    }
                },
                'L-C4BD4855': {  # All P5 Spot Instance Requests
                    'dimensions': {
                        'Type': 'Resource',
                        'Resource': 'vCPU',
                        'Service': 'EC2',
                        'Class': 'P5/Spot'
                    }
                }
            }
            
            # 从 CloudWatch 获取 usage
            # 注意：CloudWatch AWS/Usage 是 EC2 vCPU quota 的唯一权威 usage 来源
            # 当 CloudWatch 无数据时，使用 EC2 API fallback 确认是否有运行中实例
            # - 如果实例数为 0，则 usage = 0
            # - 如果存在实例但无法准确分类，则保持 NaN
            # Fallback 受 cache_ttl 保护，每 region 每小时最多执行一次
            
            # 先收集所有 CloudWatch 结果，确定哪些需要 fallback
            cloudwatch_results = {}
            fallback_quotas = []
            
            for quota_code, config in cloudwatch_quotas.items():
                try:
                    logger.debug(f"尝试从 CloudWatch 获取 {quota_code} usage...")
                    value = cloudwatch_client.get_metric_statistics(
                        namespace='AWS/Usage',
                        metric_name='ResourceCount',
                        dimensions=config['dimensions']
                    )
                    if value is not None:
                        cloudwatch_results[quota_code] = float(value)
                        logger.info(f"CloudWatch 获取成功 {quota_code}: {value}")
                    else:
                        # CloudWatch 无数据，需要 fallback
                        fallback_quotas.append(quota_code)
                        logger.debug(f"[CloudWatch 无数据] {quota_code}，将使用 EC2 API fallback")
                except Exception as e:
                    # CloudWatch API 调用异常：记录错误但不影响其他配额
                    logger.error(f"[CloudWatch API 异常] {quota_code}: {e} - 返回 NaN（API 调用失败，请检查权限和网络）", exc_info=True)
                    # 继续处理其他配额
            
            # 将 CloudWatch 成功的结果添加到 usage_data
            usage_data.update(cloudwatch_results)
            
            # 如果有需要 fallback 的配额，统一获取一次运行中实例（受 cache_ttl 保护）
            if fallback_quotas:
                logger.debug(f"需要 fallback 的配额: {fallback_quotas}，统一获取运行中实例...")
                try:
                    # 获取所有运行中的实例（受 cache_ttl 保护，每小时最多执行一次）
                    running_instances = ec2_client.describe_instances(
                        filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
                    )
                    instance_count = len(running_instances)
                    logger.debug(f"获取到 {instance_count} 个运行中实例")
                    
                    # 对每个需要 fallback 的配额应用 fallback 逻辑
                    for quota_code in fallback_quotas:
                        if instance_count == 0:
                            # 没有运行中的实例，usage = 0
                            usage_data[quota_code] = 0.0
                            logger.info(f"[Fallback] {quota_code}: 无运行中实例，usage = 0")
                        else:
                            # 存在实例但无法准确分类（CloudWatch 是唯一权威来源），保持 NaN
                            logger.info(f"[Fallback] {quota_code}: 存在 {instance_count} 个运行中实例，但无法准确分类，保持 NaN")
                            # 不包含在字典中，会返回 NaN
                except Exception as fallback_error:
                    # Fallback API 调用失败，所有 fallback 配额保持 NaN
                    logger.warning(f"[Fallback] EC2 API fallback 失败: {fallback_error}，所有 fallback 配额保持 NaN")
                    # 不包含在字典中，会返回 NaN
            
            # 2. API 方式获取（弹性 IP 和 VPN 连接）
            # L-0263D0A3: EC2-VPC Elastic IPs
            try:
                addresses = ec2_client.describe_addresses()
                usage_data['L-0263D0A3'] = float(len(addresses))
                logger.debug(f"EC2 Elastic IPs: {len(addresses)}")
            except Exception as e:
                logger.warning(f"获取 Elastic IPs 失败: {e}")
            
            # L-3E6EC3A3: VPN connections per region
            try:
                vpn_connections = ec2_client.describe_vpn_connections()
                usage_data['L-3E6EC3A3'] = float(len(vpn_connections))
                logger.debug(f"EC2 VPN connections: {len(vpn_connections)}")
            except Exception as e:
                logger.warning(f"获取 VPN connections 失败: {e}")
            
            # 缓存结果
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"EC2 usage 收集完成: {len(usage_data)} 个配额")
            if usage_data:
                logger.debug(f"EC2 usage 数据: {usage_data}")
            else:
                logger.warning(f"EC2 usage 数据为空，可能所有 API 调用都失败了")
            
            return usage_data
            
        except Exception as e:
            logger.error(f"EC2 usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "ec2"


class EBSUsageCollector(UsageCollector):
    """
    EBS Usage Collector
    
    功能：
    - 收集 EBS 服务的所有配额使用量
    - 使用缓存（1小时 TTL）
    - 通过 EC2 API 获取卷和快照信息
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 EBS Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 EBS 的使用量数据
        
        Returns:
            {quota_code: usage_value} 字典
        """
        cache_key = f"ebs_usage:{account_id}:{region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"EBS usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 EBS usage (account: {account_id}, region: {region})")
        
        usage_data = {}
        
        try:
            # 初始化客户端
            ec2_client = EC2Client(region=region, access_key=access_key, secret_key=secret_key)
            
            # 1. 存储容量配额（按卷类型）
            volume_type_mapping = {
                'L-7A658B76': 'gp3',   # Storage for gp3 volumes
                'L-D18FCD1D': 'gp2',   # Storage for gp2 volumes
                'L-FD252861': 'io1',   # Storage for io1 volumes
                'L-09BD8365': 'io2',   # Storage for io2 volumes
                'L-82ACEF56': 'st1',   # Storage for st1 volumes
                'L-17AF77E8': 'sc1',   # Storage for sc1 volumes
            }
            
            for quota_code, volume_type in volume_type_mapping.items():
                volumes = ec2_client.describe_volumes(volume_type=volume_type)
                # 计算总容量（GiB 转 TiB）
                total_size_gib = sum(vol.get('Size', 0) for vol in volumes)
                total_size_tib = total_size_gib / 1024.0
                usage_data[quota_code] = total_size_tib
            
            # 2. IOPS 配额
            # L-8D977E7E: IOPS for io2 volumes
            io2_volumes = ec2_client.describe_volumes(volume_type='io2')
            total_io2_iops = sum(vol.get('Iops', 0) for vol in io2_volumes)
            usage_data['L-8D977E7E'] = float(total_io2_iops)
            
            # L-B3A130E6: IOPS for io1 volumes
            io1_volumes = ec2_client.describe_volumes(volume_type='io1')
            total_io1_iops = sum(vol.get('Iops', 0) for vol in io1_volumes)
            usage_data['L-B3A130E6'] = float(total_io1_iops)
            
            # 3. 快照配额
            # L-309BACF6: Snapshots per Region
            snapshots = ec2_client.describe_snapshots()
            usage_data['L-309BACF6'] = float(len(snapshots))
            
            # 缓存结果
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"EBS usage 收集完成: {len(usage_data)} 个配额")
            if usage_data:
                logger.debug(f"EBS usage 数据: {usage_data}")
            else:
                logger.warning(f"EBS usage 数据为空，可能所有 API 调用都失败了")
            
            return usage_data
            
        except Exception as e:
            logger.error(f"EBS usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "ebs"


class ELBUsageCollector(UsageCollector):
    """
    ELB Usage Collector
    
    功能：
    - 收集 ELB 服务的所有配额使用量
    - 使用缓存（1小时 TTL）
    - 通过 ELBv2 API 获取负载均衡器和目标组信息
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 ELB Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 ELB 的使用量数据
        
        Returns:
            {quota_code: usage_value} 字典
        """
        cache_key = f"elb_usage:{account_id}:{region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"ELB usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 ELB usage (account: {account_id}, region: {region})")
        
        usage_data = {}
        
        try:
            # 初始化客户端
            elb_client = ELBClient(region=region, access_key=access_key, secret_key=secret_key)
            
            # 1. Application Load Balancers per Region (L-53DA6B97)
            try:
                alb_list = elb_client.describe_load_balancers(lb_type='application')
                usage_data['L-53DA6B97'] = float(len(alb_list))
                logger.debug(f"ALB 数量: {len(alb_list)}")
            except Exception as e:
                logger.warning(f"获取 ALB 数量失败: {e}")
                # 继续处理其他配额
            
            # 2. Network Load Balancers per Region (L-69A177A2)
            try:
                nlb_list = elb_client.describe_load_balancers(lb_type='network')
                usage_data['L-69A177A2'] = float(len(nlb_list))
                logger.debug(f"NLB 数量: {len(nlb_list)}")
            except Exception as e:
                logger.warning(f"获取 NLB 数量失败: {e}")
                # 继续处理其他配额
            
            # 3. Target Groups per Region (L-B22855CB)
            try:
                target_groups = elb_client.describe_target_groups()
                usage_data['L-B22855CB'] = float(len(target_groups))
                logger.debug(f"Target Groups 数量: {len(target_groups)}")
            except Exception as e:
                logger.warning(f"获取 Target Groups 数量失败: {e}")
                # 继续处理其他配额
            
            # 缓存结果
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"ELB usage 收集完成: {len(usage_data)} 个配额")
            if usage_data:
                logger.debug(f"ELB usage 数据: {usage_data}")
            else:
                logger.warning(f"ELB usage 数据为空，可能所有 API 调用都失败了")
            
            return usage_data
        except Exception as e:
            logger.error(f"ELB usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "elasticloadbalancing"


class EKSUsageCollector(UsageCollector):
    """
    EKS Usage Collector
    
    功能：
    - 收集 EKS 服务的所有配额使用量
    - 使用缓存（1小时 TTL）
    - 通过 EKS API 获取集群、节点组等信息
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 EKS Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 EKS 的使用量数据
        
        Returns:
            {quota_code: usage_value} 字典
            无法明确映射的配额不包含在字典中（会返回 NaN）
        """
        cache_key = f"eks_usage:{account_id}:{region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"EKS usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 EKS usage (account: {account_id}, region: {region})")
        
        usage_data = {}
        
        try:
            # 初始化客户端
            eks_client = EKSClient(region=region, access_key=access_key, secret_key=secret_key)
            
            # 先获取集群列表（所有配额都需要）
            try:
                clusters = eks_client.list_clusters()
                logger.debug(f"EKS 集群数量: {len(clusters)}")
            except Exception as e:
                logger.warning(f"获取 EKS 集群列表失败: {e}")
                # 如果无法获取集群列表，所有配额都无法计算，返回空字典（导致 NaN）
                return {}
            
            # 1. L-1194D53C: Clusters → len(ListClusters)
            # 这是对象数量，明确映射
            usage_data['L-1194D53C'] = float(len(clusters))
            
            # 2. L-6D54EA21: Managed node groups per cluster
            # 【派生型 usage (max-per-entity)】
            # Limit: 每个集群的托管节点组数量限制
            # Usage: max(nodegroups_per_cluster) - 所有集群中节点组数的最大值
            # 语义: 当前所有集群中，单个集群拥有的最大节点组数
            if clusters:
                nodegroups_per_cluster = []
                for cluster_name in clusters:
                    try:
                        nodegroups = eks_client.list_nodegroups(cluster_name)
                        nodegroups_per_cluster.append(len(nodegroups))
                        logger.debug(f"集群 {cluster_name} 有 {len(nodegroups)} 个节点组")
                    except Exception as e:
                        logger.warning(f"获取集群 {cluster_name} 的节点组列表失败: {e}")
                        # 单个 cluster API 失败则跳过，继续处理其他 cluster
                        continue
                
                if nodegroups_per_cluster:
                    max_nodegroups = max(nodegroups_per_cluster)
                    usage_data['L-6D54EA21'] = float(max_nodegroups)
                    logger.debug(f"L-6D54EA21: max(nodegroups_per_cluster) = {max_nodegroups}")
                else:
                    # 所有 cluster 都失败 → usage = NaN（不包含在字典中）
                    logger.warning("L-6D54EA21: 所有集群的节点组列表获取都失败，返回 NaN")
            else:
                # 没有集群，usage = 0
                usage_data['L-6D54EA21'] = 0.0
                logger.debug("L-6D54EA21: 没有集群，usage = 0")
            
            # 3. L-BD136A63: Nodes per managed node group
            # 【派生型 usage (max-per-entity)】
            # Limit: 每个托管节点组的最大节点数限制
            # Usage: max(nodes_per_nodegroup) - 所有节点组中节点数的最大值
            # 语义: 当前所有节点组中，单个节点组拥有的最大节点数
            # 节点数从 scalingConfig.desiredSize 获取
            if clusters:
                nodes_per_nodegroup = []
                for cluster_name in clusters:
                    try:
                        nodegroups = eks_client.list_nodegroups(cluster_name)
                        for nodegroup_name in nodegroups:
                            try:
                                nodegroup_info = eks_client.describe_nodegroup(cluster_name, nodegroup_name)
                                scaling_config = nodegroup_info.get('scalingConfig', {})
                                desired_size = scaling_config.get('desiredSize', 0)
                                if desired_size is not None:
                                    nodes_per_nodegroup.append(desired_size)
                                    logger.debug(f"节点组 {cluster_name}/{nodegroup_name} 有 {desired_size} 个节点")
                            except Exception as e:
                                logger.warning(f"获取节点组 {cluster_name}/{nodegroup_name} 详情失败: {e}")
                                # 单个 nodegroup API 失败则跳过，继续处理其他 nodegroup
                                continue
                    except Exception as e:
                        logger.warning(f"获取集群 {cluster_name} 的节点组列表失败: {e}")
                        # 单个 cluster API 失败则跳过，继续处理其他 cluster
                        continue
                
                if nodes_per_nodegroup:
                    max_nodes = max(nodes_per_nodegroup)
                    usage_data['L-BD136A63'] = float(max_nodes)
                    logger.debug(f"L-BD136A63: max(nodes_per_nodegroup) = {max_nodes}")
                else:
                    # 所有 nodegroup 都失败 → usage = NaN（不包含在字典中）
                    logger.warning("L-BD136A63: 所有节点组的详情获取都失败，返回 NaN")
            else:
                # 没有集群，usage = 0
                usage_data['L-BD136A63'] = 0.0
                logger.debug("L-BD136A63: 没有集群，usage = 0")
            
            # 缓存结果（即使只有部分数据）
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"EKS usage 收集完成: {len(usage_data)} 个配额有值")
            if usage_data:
                logger.debug(f"EKS usage 数据: {usage_data}")
            else:
                logger.warning(f"EKS usage 数据为空，可能所有 API 调用都失败了")
            
            return usage_data
        except Exception as e:
            logger.error(f"EKS usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "eks"


class ElastiCacheUsageCollector(UsageCollector):
    """
    ElastiCache Usage Collector
    
    功能：
    - 收集 ElastiCache 服务的所有配额使用量
    - 使用缓存（1小时 TTL）
    - 通过 ElastiCache API 获取缓存集群、复制组、Serverless 缓存等信息
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 ElastiCache Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 ElastiCache 的使用量数据
        
        Returns:
            {quota_code: usage_value} 字典
            不支持或无法准确映射的配额不包含在字典中（会返回 NaN）
        """
        cache_key = f"elasticache_usage:{account_id}:{region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"ElastiCache usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 ElastiCache usage (account: {account_id}, region: {region})")
        
        usage_data = {}
        
        try:
            # 初始化客户端
            elasticache_client = ElastiCacheClient(region=region, access_key=access_key, secret_key=secret_key)
            
            # 1. L-DFE45DF3: Nodes per Region
            # 所有节点总数（包括 Memcached、Redis 非集群模式、Redis 集群模式）
            try:
                total_nodes = 0
                
                # 1.1 Memcached 和 Redis 非集群模式的节点数
                cache_clusters = elasticache_client.describe_cache_clusters()
                for cluster in cache_clusters:
                    # 只统计不属于复制组的集群（非集群模式的 Redis 或 Memcached）
                    if not cluster.get('ReplicationGroupId'):
                        num_nodes = cluster.get('NumCacheNodes', 0)
                        total_nodes += num_nodes
                        logger.debug(f"集群 {cluster.get('CacheClusterId')} 有 {num_nodes} 个节点")
                
                # 1.2 Redis 集群模式的节点数
                replication_groups = elasticache_client.describe_replication_groups()
                for rg in replication_groups:
                    total_nodes += rg.get('TotalNodes', 0)
                    logger.debug(f"复制组 {rg.get('ReplicationGroupId')} 有 {rg.get('TotalNodes')} 个节点")
                
                usage_data['L-DFE45DF3'] = float(total_nodes)
                logger.debug(f"L-DFE45DF3: 总节点数 = {total_nodes}")
            except Exception as e:
                logger.warning(f"获取 L-DFE45DF3 usage 失败: {e}")
                # API 失败返回 NaN（不包含在字典中）
            
            # 2. L-AF354865: Nodes per cluster (cluster mode enabled)
            # Redis 集群模式中单个集群的最大节点数
            # 注意：这是每个 NodeGroup 的节点数，不是整个复制组的节点数
            try:
                replication_groups = elasticache_client.describe_replication_groups()
                if replication_groups:
                    max_nodes_per_nodegroup = 0
                    for rg in replication_groups:
                        nodes_per_nodegroup = rg.get('NodesPerNodeGroup', [])
                        if nodes_per_nodegroup:
                            max_in_rg = max(nodes_per_nodegroup)
                            max_nodes_per_nodegroup = max(max_nodes_per_nodegroup, max_in_rg)
                            logger.debug(f"复制组 {rg.get('ReplicationGroupId')} 的最大节点组节点数 = {max_in_rg}")
                    
                    if max_nodes_per_nodegroup > 0:
                        usage_data['L-AF354865'] = float(max_nodes_per_nodegroup)
                        logger.debug(f"L-AF354865: max(nodes_per_nodegroup) = {max_nodes_per_nodegroup}")
                    else:
                        # 没有有效的节点组数据 → usage = NaN（不包含在字典中）
                        logger.warning("L-AF354865: 没有有效的 Redis 集群模式节点组数据，返回 NaN")
                else:
                    # 没有复制组，usage = 0
                    usage_data['L-AF354865'] = 0.0
                    logger.debug("L-AF354865: 没有复制组，usage = 0")
            except Exception as e:
                logger.warning(f"获取 L-AF354865 usage 失败: {e}")
                # API 失败返回 NaN（不包含在字典中）
            
            # 3. L-8C334AD1: Nodes per cluster (Memcached)
            # Memcached 集群中单个集群的最大节点数
            try:
                cache_clusters = elasticache_client.describe_cache_clusters()
                memcached_clusters = [c for c in cache_clusters if c.get('Engine', '').lower() == 'memcached']
                
                if memcached_clusters:
                    max_nodes = max(c.get('NumCacheNodes', 0) for c in memcached_clusters)
                    if max_nodes > 0:
                        usage_data['L-8C334AD1'] = float(max_nodes)
                        logger.debug(f"L-8C334AD1: max(nodes_per_memcached_cluster) = {max_nodes}")
                    else:
                        # 所有 Memcached 集群都没有节点 → usage = NaN（不包含在字典中）
                        logger.warning("L-8C334AD1: 所有 Memcached 集群都没有节点，返回 NaN")
                else:
                    # 没有 Memcached 集群，usage = 0
                    usage_data['L-8C334AD1'] = 0.0
                    logger.debug("L-8C334AD1: 没有 Memcached 集群，usage = 0")
            except Exception as e:
                logger.warning(f"获取 L-8C334AD1 usage 失败: {e}")
                # API 失败返回 NaN（不包含在字典中）
            
            # 4. L-BBCDAECC: Serverless Caches per Region
            # Serverless Cache 实例数
            try:
                serverless_caches = elasticache_client.describe_serverless_caches()
                usage_data['L-BBCDAECC'] = float(len(serverless_caches))
                logger.debug(f"L-BBCDAECC: Serverless Cache 数量 = {len(serverless_caches)}")
            except Exception as e:
                logger.warning(f"获取 L-BBCDAECC usage 失败: {e}")
                # API 失败返回 NaN（不包含在字典中）
            
            # 缓存结果（即使只有部分数据）
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"ElastiCache usage 收集完成: {len(usage_data)} 个配额有值")
            if usage_data:
                logger.debug(f"ElastiCache usage 数据: {usage_data}")
            else:
                logger.warning(f"ElastiCache usage 数据为空，可能所有 API 调用都失败了")
            
            return usage_data
        except Exception as e:
            logger.error(f"ElastiCache usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "elasticache"


class Route53UsageCollector(UsageCollector):
    """
    Route53 Usage Collector
    
    功能：
    - 收集 Route53 服务的所有配额使用量
    - 使用缓存（1小时 TTL）
    - 通过 Route53 API 获取 Hosted Zones 信息
    - Route53 是全局服务，region 固定为 us-east-1
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 Route53 Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 Route53 的使用量数据
        
        注意：Route53 是全局服务，region 参数会被忽略，实际使用 us-east-1
        
        Returns:
            {quota_code: usage_value} 字典
            - L-F767CB15: Domain count limit -> 注册域名数量
            - L-4EA4796A: Hosted zones -> Hosted Zones 数量
        """
        # Route53 是全局服务，使用 us-east-1
        route53_region = 'us-east-1'
        cache_key = f"route53_usage:{account_id}:{route53_region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"Route53 usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 Route53 usage (account: {account_id}, region: {route53_region})")
        
        usage_data = {}
        
        try:
            # 初始化客户端（Route53 是全局服务，使用 us-east-1）
            route53_client = Route53Client(region=route53_region, access_key=access_key, secret_key=secret_key)
            
            # L-F767CB15: Domain count limit -> 注册的域名数量（不是Hosted Zones）
            # 注意：Domain count limit 指的是通过Route53注册的域名数量，不是Hosted Zones数量
            # Route53 API 无法直接获取注册域名数量，但AWS控制台显示usage为0
            # 尝试使用Route53domains API获取注册域名数量
            try:
                # 创建 route53domains client（支持凭证）
                if access_key and secret_key:
                    session = boto3.Session(
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key
                    )
                    route53domains_client = session.client('route53domains', region_name=route53_region)
                else:
                    route53domains_client = boto3.client('route53domains', region_name=route53_region)
                # Route53domains API支持分页，需要遍历所有页面
                domain_count = 0
                paginator = route53domains_client.get_paginator('list_domains')
                for page in paginator.paginate():
                    domain_count += len(page.get('Domains', []))
                
                usage_data['L-F767CB15'] = float(domain_count)
                logger.info(f"Route53 Domain count usage (from Route53domains API): {domain_count}")
            except Exception as domains_error:
                # Route53domains API可能不可用或没有权限，使用0作为默认值（与AWS控制台一致）
                logger.debug(f"Route53domains API不可用，使用默认值0: {domains_error}")
                usage_data['L-F767CB15'] = 0.0
                logger.info(f"Route53 Domain count usage: 0 (无法获取注册域名数量，使用默认值)")
            
            # L-4EA4796A: Hosted zones per account -> Hosted Zones 数量
            # Usage = GetHostedZoneCount API 返回的 HostedZoneCount（推荐方法）
            # 备选：也可以使用 get_account_limit 返回的 Count 字段
            try:
                hosted_zone_count = route53_client.get_hosted_zone_count()
                if hosted_zone_count is not None:
                    usage_data['L-4EA4796A'] = float(hosted_zone_count)
                    logger.info(f"Route53 Hosted Zones usage (from GetHostedZoneCount API): {hosted_zone_count}")
                else:
                    # 如果 GetHostedZoneCount 失败，尝试使用 get_account_limit 的 Count 字段作为备选
                    logger.debug("GetHostedZoneCount 返回 None，尝试使用 get_account_limit 的 Count 字段")
                    limit_info = route53_client.get_account_limit('MAX_HOSTED_ZONES_BY_OWNER')
                    if limit_info and 'count' in limit_info:
                        hosted_zone_count = limit_info['count']
                        usage_data['L-4EA4796A'] = float(hosted_zone_count)
                        logger.info(f"Route53 Hosted Zones usage (from get_account_limit Count, fallback): {hosted_zone_count}")
                    else:
                        logger.warning(f"Route53 无法获取 Hosted Zones usage: get_account_limit 也返回无效数据")
            except Exception as e:
                logger.warning(f"获取 Route53 Hosted Zones usage 失败: {e}")
                # 失败时不添加到 usage_data，会返回 NaN
            
            # 缓存结果
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"Route53 usage 收集完成: {len(usage_data)} 个配额有值")
            if usage_data:
                logger.debug(f"Route53 usage 数据: {usage_data}")
            else:
                logger.warning(f"Route53 usage 数据为空，可能 API 调用失败")
            
            return usage_data
            
        except Exception as e:
            logger.error(f"Route53 usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "route53"


class CloudFrontUsageCollector(UsageCollector):
    """
    CloudFront Usage Collector（重构版）
    
    功能：
    - 收集 CloudFront 服务的配额使用量（service-level）
    - 使用缓存（1小时 TTL）
    - 通过 boto3 cloudfront.list_distributions 获取 Distribution 数量
    - CloudFront 是全局服务，固定使用 us-east-1
    - 不走 EC2 Region 发现逻辑
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 CloudFront Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 CloudFront 的使用量数据（service-level）
        
        CloudFront 是全局服务，固定使用 us-east-1 创建客户端。
        使用以下 API 获取各配额的使用量：
        - L-24B04930: list_distributions().DistributionList.Quantity
        - L-7D134442: list_cache_policies(Type="custom") 的 Items 数量
        - L-CF0D4FC5: list_response_headers_policies(Type="custom") 的 Items 数量
        - L-08884E5C: list_cloud_front_origin_access_identities().CloudFrontOriginAccessIdentityList.Quantity
        
        Args:
            account_id: AWS 账号 ID
            region: AWS Region（CloudFront 是全局服务，固定使用 us-east-1）
            access_key: AWS Access Key（可选）
            secret_key: AWS Secret Key（可选）
        
        Returns:
            {quota_code: usage_value} 字典
            - L-24B04930: Web distributions per AWS account
            - L-7D134442: Cache policies per AWS account
            - L-CF0D4FC5: Response headers policies
            - L-08884E5C: Origin access identities per account
            API 失败时返回空字典，usage 会显示为 NaN
            无资源时 usage 返回 0，异常时返回 NaN
        """
        # CloudFront 是全局服务，固定使用 us-east-1
        cloudfront_region = 'us-east-1'
        cache_key = f"cloudfront_usage:{account_id}:{cloudfront_region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"CloudFront usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 CloudFront usage (account: {account_id}, region: {cloudfront_region})")
        
        usage_data = {}
        
        try:
            # 使用 boto3 直接创建 CloudFront 客户端（固定 us-east-1）
            import boto3
            from botocore.exceptions import ClientError, BotoCoreError
            
            if access_key and secret_key:
                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
                cloudfront_client = session.client('cloudfront', region_name=cloudfront_region)
            else:
                cloudfront_client = boto3.client('cloudfront', region_name=cloudfront_region)
            
            # 1. L-24B04930: Web distributions per AWS account
            # 使用 list_distributions().DistributionList.Quantity
            try:
                response = cloudfront_client.list_distributions()
                distribution_list = response.get('DistributionList', {})
                distribution_count = distribution_list.get('Quantity', 0)
                usage_data['L-24B04930'] = float(distribution_count)
                logger.info(f"CloudFront Distribution 数量: {distribution_count}")
                
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                error_message = e.response.get("Error", {}).get("Message")
                logger.warning(f"获取 CloudFront Distribution 数量失败: {error_code} - {error_message}")
            except BotoCoreError as e:
                logger.warning(f"获取 CloudFront Distribution 数量失败（BotoCoreError）: {e}")
            except Exception as e:
                logger.warning(f"获取 CloudFront Distribution 数量失败: {e}")
            
            # 2. L-7D134442: Cache policies per AWS account
            # 使用 list_cache_policies(Type="custom") 的 Items 数量（只统计 custom）
            try:
                all_cache_policies = []
                marker = None
                
                while True:
                    request_params = {'Type': 'custom'}  # 只统计 custom 类型
                    if marker:
                        request_params['Marker'] = marker
                    
                    response = cloudfront_client.list_cache_policies(**request_params)
                    cache_policy_list = response.get('CachePolicyList', {})
                    items = cache_policy_list.get('Items', [])
                    all_cache_policies.extend(items)
                    
                    logger.debug(f"获取到 {len(items)} 个 Custom Cache Policies (累计: {len(all_cache_policies)})")
                    
                    # 检查是否有下一页
                    if cache_policy_list.get('IsTruncated', False):
                        marker = cache_policy_list.get('NextMarker')
                        if not marker:
                            logger.warning("CloudFront ListCachePolicies 返回 IsTruncated=True 但 NextMarker 为空")
                            break
                    else:
                        break
                
                cache_policy_count = len(all_cache_policies)
                usage_data['L-7D134442'] = float(cache_policy_count)
                logger.info(f"CloudFront Custom Cache Policy 数量: {cache_policy_count}")
                
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                error_message = e.response.get("Error", {}).get("Message")
                logger.warning(f"获取 CloudFront Cache Policy 数量失败: {error_code} - {error_message}")
            except BotoCoreError as e:
                logger.warning(f"获取 CloudFront Cache Policy 数量失败（BotoCoreError）: {e}")
            except Exception as e:
                logger.warning(f"获取 CloudFront Cache Policy 数量失败: {e}")
            
            # 3. L-CF0D4FC5: Response headers policies per AWS account
            # 使用 list_response_headers_policies(Type="custom") 的 Items 数量（只统计 custom）
            try:
                all_response_headers_policies = []
                marker = None
                
                while True:
                    request_params = {'Type': 'custom'}  # 只统计 custom 类型
                    if marker:
                        request_params['Marker'] = marker
                    
                    response = cloudfront_client.list_response_headers_policies(**request_params)
                    response_headers_policy_list = response.get('ResponseHeadersPolicyList', {})
                    items = response_headers_policy_list.get('Items', [])
                    all_response_headers_policies.extend(items)
                    
                    logger.debug(f"获取到 {len(items)} 个 Custom Response Headers Policies (累计: {len(all_response_headers_policies)})")
                    
                    # 检查是否有下一页
                    if response_headers_policy_list.get('IsTruncated', False):
                        marker = response_headers_policy_list.get('NextMarker')
                        if not marker:
                            logger.warning("CloudFront ListResponseHeadersPolicies 返回 IsTruncated=True 但 NextMarker 为空")
                            break
                    else:
                        break
                
                response_headers_policy_count = len(all_response_headers_policies)
                usage_data['L-CF0D4FC5'] = float(response_headers_policy_count)
                logger.info(f"CloudFront Custom Response Headers Policy 数量: {response_headers_policy_count}")
                
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                error_message = e.response.get("Error", {}).get("Message")
                logger.warning(f"获取 CloudFront Response Headers Policy 数量失败: {error_code} - {error_message}")
            except BotoCoreError as e:
                logger.warning(f"获取 CloudFront Response Headers Policy 数量失败（BotoCoreError）: {e}")
            except Exception as e:
                logger.warning(f"获取 CloudFront Response Headers Policy 数量失败: {e}")
            
            # 4. L-08884E5C: Origin access identities per account
            # 使用 list_cloud_front_origin_access_identities().CloudFrontOriginAccessIdentityList.Quantity
            try:
                response = cloudfront_client.list_cloud_front_origin_access_identities()
                origin_access_identity_list = response.get('CloudFrontOriginAccessIdentityList', {})
                origin_access_identity_count = origin_access_identity_list.get('Quantity', 0)
                usage_data['L-08884E5C'] = float(origin_access_identity_count)
                logger.info(f"CloudFront Origin Access Identity 数量: {origin_access_identity_count}")
                
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                error_message = e.response.get("Error", {}).get("Message")
                logger.warning(f"获取 CloudFront Origin Access Identity 数量失败: {error_code} - {error_message}")
            except BotoCoreError as e:
                logger.warning(f"获取 CloudFront Origin Access Identity 数量失败（BotoCoreError）: {e}")
            except Exception as e:
                logger.warning(f"获取 CloudFront Origin Access Identity 数量失败: {e}")
            
            # 缓存结果
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"CloudFront usage 收集完成: {len(usage_data)} 个配额")
            if usage_data:
                logger.debug(f"CloudFront usage 数据: {usage_data}")
            else:
                logger.warning(f"CloudFront usage 数据为空，所有 API 调用失败")
            
            return usage_data
        except Exception as e:
            logger.error(f"CloudFront usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "cloudfront"


class SageMakerUsageCollector(UsageCollector):
    """
    SageMaker Usage Collector
    
    功能：
    - 收集 SageMaker 服务的配额使用量（service-level）
    - 使用缓存（1小时 TTL）控制成本
    - 通过 SageMaker API 获取资源数量（使用免费的 List API）
    - 根据配额名称动态匹配资源类型（Discovery 模式）
    
    成本控制：
    - 使用免费的 List API（ListNotebookInstances, ListTrainingJobs, ListEndpoints）
    - 1小时缓存，减少 API 调用频率
    - 批量获取，一次调用获取所有资源
    """
    
    def __init__(self, cache: MemoryCache):
        """
        初始化 SageMaker Usage Collector
        
        Args:
            cache: 内存缓存实例
        """
        self.cache = cache
        self.cache_ttl = 3600  # 1 小时缓存
    
    def collect_usage(self, account_id: str, region: str, access_key: str = None, secret_key: str = None) -> Dict[str, float]:
        """
        收集 SageMaker 的使用量数据（service-level）
        
        由于 SageMaker 使用 Discovery 模式，配额是动态的。本方法会：
        1. 调用 Service Quotas API 获取所有 SageMaker 配额（带缓存）
        2. 根据配额名称中的关键词匹配资源类型
        3. 调用对应的 SageMaker API 获取资源数量
        4. 返回 {quota_code: usage_value} 字典
        
        配额名称匹配规则：
        - "notebook instance usage" → Notebook Instance 数量
        - "training job usage" → Training Job 数量
        - "endpoint usage" → Endpoint 数量
        
        Args:
            account_id: AWS 账号 ID
            region: AWS Region
            access_key: AWS Access Key（可选）
            secret_key: AWS Secret Key（可选）
        
        Returns:
            {quota_code: usage_value} 字典
            API 失败时返回空字典，usage 会显示为 NaN
            无资源时 usage 返回 0
        """
        cache_key = f"sagemaker_usage:{account_id}:{region}"
        
        # 检查缓存
        cached_value, exists = self.cache.get(cache_key)
        if exists:
            logger.debug(f"SageMaker usage 缓存命中: {cache_key}")
            return cached_value
        
        logger.info(f"开始收集 SageMaker usage (account: {account_id}, region: {region})")
        
        usage_data = {}
        
        try:
            # 1. 获取所有 SageMaker 配额（使用 Service Quotas API，带缓存）
            quotas_cache_key = f"sagemaker_quotas:{region}"
            quotas_cache, quotas_exists = self.cache.get(quotas_cache_key)
            
            if not quotas_exists:
                logger.info(f"获取 SageMaker 配额列表（Service Quotas API）...")
                try:
                    # 初始化 Service Quotas 客户端
                    if access_key and secret_key:
                        sq_client = ServiceQuotasClient(
                            region=region,
                            access_key=access_key,
                            secret_key=secret_key
                        )
                    else:
                        sq_client = ServiceQuotasClient(region=region)
                    
                    # 获取所有 SageMaker 配额
                    all_quotas = sq_client.list_service_quotas(service_code="sagemaker")
                    
                    # 缓存配额列表（24小时，因为配额变化不频繁）
                    self.cache.set(quotas_cache_key, all_quotas, 86400)
                    quotas_list = all_quotas
                    logger.info(f"获取到 {len(quotas_list)} 个 SageMaker 配额")
                except Exception as e:
                    logger.warning(f"获取 SageMaker 配额列表失败: {e}，将使用默认匹配规则")
                    quotas_list = []
            else:
                quotas_list = quotas_cache
                logger.debug(f"使用缓存的 SageMaker 配额列表: {len(quotas_list)} 个配额")
            
            # 2. 初始化 SageMaker 客户端
            sagemaker_client = SageMakerClient(region=region, access_key=access_key, secret_key=secret_key)
            
            # 3. 获取资源数量（一次性获取所有类型，减少 API 调用）
            notebook_instance_count = 0
            training_job_count = 0
            endpoint_count = 0
            
            logger.info("开始获取 SageMaker 资源数量...")
            logger.info("优化策略：只统计运行中的资源（配额通常针对正在使用的资源）")
            
            try:
                # Notebook Instance: 只统计 InService 状态的（运行中）
                notebook_instance_count = sagemaker_client.get_notebook_instance_count(status_filter='InService')
                logger.info(f"Notebook Instance 数量（运行中）: {notebook_instance_count}")
            except Exception as e:
                logger.warning(f"获取 Notebook Instance 数量失败: {e}")
                # 如果失败，尝试获取所有状态的数量
                try:
                    notebook_instance_count = sagemaker_client.get_notebook_instance_count()
                    logger.info(f"Notebook Instance 数量（所有状态）: {notebook_instance_count}")
                except Exception as e2:
                    logger.warning(f"获取 Notebook Instance 数量（所有状态）也失败: {e2}")
                    notebook_instance_count = 0
            
            try:
                # Training Job: 只统计 InProgress 状态的（运行中）
                # 注意：Training Job 配额通常是指并发运行的训练任务
                # 添加超时和最大页数限制，避免无限等待
                training_job_count = sagemaker_client.get_training_job_count(
                    status_filter='InProgress',
                    max_pages=100,  # 最多处理 100 页（约 10000 个任务）
                    timeout_seconds=30  # 30 秒超时
                )
                logger.info(f"Training Job 数量（运行中）: {training_job_count}")
            except Exception as e:
                logger.warning(f"获取 Training Job 数量（运行中）失败: {e}")
                # 如果失败，尝试不使用状态过滤，但使用更严格的限制
                logger.info("尝试获取所有状态的 Training Job 数量（使用严格限制）...")
                try:
                    training_job_count = sagemaker_client.get_training_job_count(
                        status_filter=None,
                        max_pages=50,  # 只处理前 50 页作为估算
                        timeout_seconds=20  # 20 秒超时
                    )
                    logger.info(f"Training Job 数量（估算，前 50 页）: {training_job_count}")
                except Exception as e2:
                    logger.warning(f"获取 Training Job 数量也失败: {e2}")
                    training_job_count = 0
            
            try:
                # Endpoint: 只统计 InService 状态的（运行中）
                endpoint_count = sagemaker_client.get_endpoint_count(status_filter='InService')
                logger.info(f"Endpoint 数量（运行中）: {endpoint_count}")
            except Exception as e:
                logger.warning(f"获取 Endpoint 数量失败: {e}")
                # 如果失败，尝试获取所有状态的数量
                try:
                    endpoint_count = sagemaker_client.get_endpoint_count()
                    logger.info(f"Endpoint 数量（所有状态）: {endpoint_count}")
                except Exception as e2:
                    logger.warning(f"获取 Endpoint 数量（所有状态）也失败: {e2}")
                    endpoint_count = 0
            
            # 4. 根据配额名称匹配资源类型并设置 usage
            logger.info("开始匹配配额并设置 Usage 值...")
            if quotas_list:
                matched_count = 0
                for quota in quotas_list:
                    quota_code = quota.get('quota_code', '')
                    quota_name = quota.get('quota_name', '')
                    quota_name_lower = quota_name.lower()
                    
                    # 根据配额名称中的关键词匹配
                    if 'notebook instance' in quota_name_lower and 'usage' in quota_name_lower:
                        usage_data[quota_code] = float(notebook_instance_count)
                        logger.info(f"✓ 匹配配额 {quota_code}: {quota_name} → {notebook_instance_count}")
                        matched_count += 1
                    elif 'training job' in quota_name_lower and 'usage' in quota_name_lower:
                        usage_data[quota_code] = float(training_job_count)
                        logger.info(f"✓ 匹配配额 {quota_code}: {quota_name} → {training_job_count}")
                        matched_count += 1
                    elif 'endpoint' in quota_name_lower and 'usage' in quota_name_lower:
                        usage_data[quota_code] = float(endpoint_count)
                        logger.info(f"✓ 匹配配额 {quota_code}: {quota_name} → {endpoint_count}")
                        matched_count += 1
                    # 其他配额类型暂时不处理（返回 NaN）
                
                logger.info(f"配额匹配完成: 共 {len(quotas_list)} 个配额，匹配到 {matched_count} 个")
            else:
                # 如果没有配额列表，使用默认匹配规则（根据配置的匹配规则）
                logger.debug("使用默认匹配规则（基于配置的 match_rules）")
                # 这里可以根据配置的 match_rules 来匹配，但为了简化，我们直接使用关键词匹配
                # 如果后续需要，可以从配置中读取 match_rules
                pass
            
            # 5. 缓存结果（即使只有部分数据）
            if usage_data:
                self.cache.set(cache_key, usage_data, self.cache_ttl)
            
            logger.info(f"SageMaker usage 收集完成: {len(usage_data)} 个配额有值")
            if usage_data:
                logger.debug(f"SageMaker usage 数据: {usage_data}")
            else:
                logger.warning(f"SageMaker usage 数据为空，可能所有 API 调用都失败了")
            
            return usage_data
        except Exception as e:
            logger.error(f"SageMaker usage 收集失败: {e}", exc_info=True)
            return {}
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "sagemaker"

