# -*- coding: utf-8 -*-
"""
SageMaker 配额动态发现模块

功能：
- 通过 ListServiceQuotas API 动态发现 SageMaker 配额
- 根据配置的匹配规则进行模糊匹配
- 将发现的配额转换为统一的 QuotaItem 结构
"""

import logging
from typing import List, Dict, Optional
from config.loader import QuotaItem, DiscoveryConfig

logger = logging.getLogger(__name__)


class SageMakerDiscovery:
    """
    SageMaker 配额发现器
    
    功能：
    - 调用 AWS Service Quotas API 获取所有 SageMaker 配额
    - 根据匹配规则过滤配额
    - 返回匹配的配额列表
    """
    
    def __init__(self, service_quotas_client, discovery_config: DiscoveryConfig):
        """
        初始化 SageMaker Discovery
        
        Args:
            service_quotas_client: AWS Service Quotas API 客户端
            discovery_config: Discovery 配置对象
        """
        self.client = service_quotas_client
        self.config = discovery_config
    
    def discover_quotas(self, region: str) -> List[QuotaItem]:
        """
        发现匹配的 SageMaker 配额
        
        Args:
            region: AWS 区域
        
        Returns:
            匹配的 QuotaItem 列表
        """
        logger.debug(f"[SageMaker Discovery] 开始发现配额，区域: {region}")
        logger.debug(f"[SageMaker Discovery] 匹配规则: {self.config.match_rules}")
        
        try:
            # 调用 Service Quotas API 获取所有 SageMaker 配额
            logger.debug(f"[SageMaker Discovery] 调用 ListServiceQuotas(serviceCode='sagemaker', region='{region}')")
            all_quotas = self.client.list_service_quotas(service_code="sagemaker")
            
            logger.debug(f"[SageMaker Discovery] API 返回 {len(all_quotas)} 个配额，开始匹配...")
            
            # 应用匹配规则
            matched_quotas = []
            for quota in all_quotas:
                quota_name = quota.get('quota_name', '')
                logger.debug(f"[SageMaker Discovery] 检查配额: {quota_name}")
                
                if self._matches_rules(quota_name):
                    logger.debug(f"[SageMaker Discovery] ✓ 配额匹配: {quota_name}")
                    quota_item = QuotaItem(
                        quota_code=quota.get('quota_code', ''),
                        quota_name=quota_name,
                        description=f"Discovered SageMaker quota: {quota_name}",
                        priority=self.config.default_priority
                    )
                    matched_quotas.append(quota_item)
                    logger.debug(f"[SageMaker Discovery]   配额代码: {quota_item.quota_code}")
                    logger.debug(f"[SageMaker Discovery]   优先级: {quota_item.priority}")
                else:
                    logger.debug(f"[SageMaker Discovery] ✗ 配额不匹配: {quota_name}")
            
            logger.info(f"[SageMaker Discovery] 区域 {region}: 发现 {len(matched_quotas)} 个匹配的配额")
            
            # 详细输出每个匹配的配额
            if matched_quotas:
                logger.info(f"[SageMaker Discovery] 匹配的配额列表:")
                for idx, quota in enumerate(matched_quotas, 1):
                    logger.info(f"  {idx}. {quota.quota_code}: {quota.quota_name}")
            
            return matched_quotas
            
        except Exception as e:
            logger.error(f"[SageMaker Discovery] 发现失败，区域 {region}: {e}", exc_info=True)
            # Discovery 失败不影响其他服务
            return []
    
    def _matches_rules(self, quota_name: str) -> bool:
        """
        检查配额名称是否匹配配置的规则
        
        Args:
            quota_name: 配额名称
        
        Returns:
            如果匹配任何规则返回 True，否则返回 False
        """
        quota_name_lower = quota_name.lower()
        
        for rule_idx, rule in enumerate(self.config.match_rules, 1):
            if 'name_contains' in rule:
                keywords = rule['name_contains']
                logger.debug(f"[SageMaker Discovery] 规则 {rule_idx}: 检查关键词 {keywords}")
                
                # 检查配额名称是否包含所有关键词
                matches = all(keyword.lower() in quota_name_lower for keyword in keywords)
                if matches:
                    logger.debug(f"[SageMaker Discovery] 规则 {rule_idx}: ✓ 匹配成功")
                    return True
                else:
                    logger.debug(f"[SageMaker Discovery] 规则 {rule_idx}: ✗ 不匹配")
        
        return False


def create_quota_items_from_discovery(discovered_quotas: List[Dict], discovery_config: DiscoveryConfig) -> List[QuotaItem]:
    """
    将发现的配额转换为 QuotaItem 列表
    
    Args:
        discovered_quotas: 从 API 发现的配额列表（包含 quota_code, quota_name 等字段）
        discovery_config: Discovery 配置
    
    Returns:
        QuotaItem 列表
    """
    quota_items = []
    
    for quota in discovered_quotas:
        quota_item = QuotaItem(
            quota_code=quota.get('quota_code', ''),
            quota_name=quota.get('quota_name', ''),
            description=f"Discovered quota: {quota.get('quota_name', '')}",
            priority=discovery_config.default_priority
        )
        quota_items.append(quota_item)
    
    return quota_items

