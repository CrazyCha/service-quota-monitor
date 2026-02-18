# -*- coding: utf-8 -*-
"""
配额配置加载模块

功能：
- 从 YAML 文件加载配额配置
- 定义清晰的数据结构（QuotaConfig / QuotaItem）
- 读取失败时给出明确错误
"""

import yaml
import os
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class QuotaItem:
    """单个配额项的数据结构"""
    quota_code: str      # 配额代码，如 "L-1216C47A"
    quota_name: str      # 配额名称
    description: str     # 配额描述
    priority: str        # 优先级：high, medium, low, critical
    cache_ttl_limit: int = 86400  # Limit 采集频率（秒），默认 24 小时
    cache_ttl_usage: int = 3600   # Usage 采集频率（秒），默认 1 小时


@dataclass
class DiscoveryConfig:
    """Discovery 配置数据结构（用于 SageMaker 等动态配额）"""
    enabled: bool                    # 是否启用 discovery
    match_rules: List[Dict]          # 匹配规则列表
    default_priority: str            # 默认优先级


@dataclass
class ServiceQuotas:
    """某个服务的配额列表"""
    service: str                    # 服务代码，如 "ec2", "rds"
    quotas: List[QuotaItem]         # 配额列表


@dataclass
class QuotaConfig:
    """配额配置的根数据结构"""
    aws: Dict[str, any]     # AWS 服务的配额配置，key 是服务代码
                              # 值可以是 List[QuotaItem]（声明型）或 Dict（discovery 配置）
    aliyun: Dict[str, List[QuotaItem]]  # Aliyun 服务的配额配置（可选）


def load_quota_config(quotas_path: str) -> QuotaConfig:
    """
    从 YAML 文件加载配额配置
    
    Args:
        quotas_path: 配额配置文件路径（如 'config/quotas.yaml'）
    
    Returns:
        QuotaConfig 对象，包含所有服务的配额配置
    
    Raises:
        FileNotFoundError: 文件不存在
        yaml.YAMLError: YAML 解析错误
        ValueError: 配置格式错误
    """
    # 检查文件是否存在
    if not os.path.exists(quotas_path):
        raise FileNotFoundError(f"配额配置文件不存在: {quotas_path}")
    
    # 读取文件内容
    try:
        with open(quotas_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except IOError as e:
        raise IOError(f"无法读取配额配置文件 {quotas_path}: {e}")
    
    # 解析 YAML
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"YAML 解析失败: {e}")
    
    if data is None:
        raise ValueError("配额配置文件为空")
    
    # 解析 AWS 配额配置
    aws_quotas = {}
    if 'aws' in data:
        aws_data = data['aws']
        if not isinstance(aws_data, dict):
            raise ValueError("配置格式错误: 'aws' 必须是字典类型")
        
        for service, service_config in aws_data.items():
            # 检查是否是 discovery 配置
            if isinstance(service_config, dict) and 'discovery' in service_config:
                # Discovery 模式：保存 discovery 配置
                discovery_config = service_config['discovery']
                if not isinstance(discovery_config, dict):
                    raise ValueError(f"配置格式错误: 'aws.{service}.discovery' 必须是字典类型")
                
                try:
                    discovery = _parse_discovery_config(discovery_config, service)
                    aws_quotas[service] = {'discovery': discovery}
                except (KeyError, ValueError) as e:
                    raise ValueError(f"配置格式错误: 'aws.{service}.discovery': {e}")
            else:
                # 声明型配置：解析配额列表
                if not isinstance(service_config, list):
                    raise ValueError(f"配置格式错误: 'aws.{service}' 必须是列表类型或包含 discovery 配置的字典")
                
                quota_items = []
                for idx, quota_dict in enumerate(service_config):
                    try:
                        quota_item = _parse_quota_item(quota_dict, service, idx)
                        quota_items.append(quota_item)
                    except (KeyError, ValueError) as e:
                        raise ValueError(f"配置格式错误: 'aws.{service}[{idx}]': {e}")
                
                aws_quotas[service] = quota_items
    
    # 解析 Aliyun 配额配置（可选）
    aliyun_quotas = {}
    if 'aliyun' in data:
        aliyun_data = data['aliyun']
        if not isinstance(aliyun_data, dict):
            raise ValueError("配置格式错误: 'aliyun' 必须是字典类型")
        
        for service, quotas_list in aliyun_data.items():
            if not isinstance(quotas_list, list):
                raise ValueError(f"配置格式错误: 'aliyun.{service}' 必须是列表类型")
            
            quota_items = []
            for idx, quota_dict in enumerate(quotas_list):
                try:
                    quota_item = _parse_quota_item(quota_dict, service, idx)
                    quota_items.append(quota_item)
                except (KeyError, ValueError) as e:
                    raise ValueError(f"配置格式错误: 'aliyun.{service}[{idx}]': {e}")
            
            aliyun_quotas[service] = quota_items
    
    return QuotaConfig(aws=aws_quotas, aliyun=aliyun_quotas)


def _parse_discovery_config(discovery_dict: dict, service: str) -> DiscoveryConfig:
    """
    解析 Discovery 配置
    
    Args:
        discovery_dict: Discovery 配置字典
        service: 服务代码（用于错误提示）
    
    Returns:
        DiscoveryConfig 对象
    
    Raises:
        KeyError: 缺少必填字段
        ValueError: 字段值无效
    """
    if 'enabled' not in discovery_dict:
        raise KeyError("缺少必填字段: enabled")
    
    enabled = discovery_dict['enabled']
    if not isinstance(enabled, bool):
        raise ValueError("enabled 必须是布尔值")
    
    if not enabled:
        raise ValueError("discovery.enabled 必须为 true")
    
    if 'match_rules' not in discovery_dict:
        raise KeyError("缺少必填字段: match_rules")
    
    match_rules = discovery_dict['match_rules']
    if not isinstance(match_rules, list):
        raise ValueError("match_rules 必须是列表类型")
    
    # 验证 match_rules 格式
    for idx, rule in enumerate(match_rules):
        if not isinstance(rule, dict):
            raise ValueError(f"match_rules[{idx}] 必须是字典类型")
        if 'name_contains' not in rule:
            raise ValueError(f"match_rules[{idx}] 缺少必填字段: name_contains")
        if not isinstance(rule['name_contains'], list):
            raise ValueError(f"match_rules[{idx}].name_contains 必须是列表类型")
    
    default_priority = discovery_dict.get('default_priority', 'high')
    if not isinstance(default_priority, str):
        raise ValueError("default_priority 必须是字符串")
    
    valid_priorities = ['high', 'medium', 'low', 'critical']
    if default_priority.lower() not in valid_priorities:
        raise ValueError(f"default_priority 必须是以下值之一: {', '.join(valid_priorities)}")
    
    return DiscoveryConfig(
        enabled=enabled,
        match_rules=match_rules,
        default_priority=default_priority.lower()
    )


def _parse_quota_item(quota_dict: dict, service: str, index: int) -> QuotaItem:
    """
    解析单个配额项
    
    Args:
        quota_dict: 配额字典数据
        service: 服务代码（用于错误提示）
        index: 索引（用于错误提示）
    
    Returns:
        QuotaItem 对象
    
    Raises:
        KeyError: 缺少必填字段
        ValueError: 字段值无效
    """
    # 检查必填字段
    required_fields = ['quota_code', 'quota_name', 'description', 'priority']
    for field in required_fields:
        if field not in quota_dict:
            raise KeyError(f"缺少必填字段: {field}")
    
    quota_code = quota_dict['quota_code']
    quota_name = quota_dict['quota_name']
    description = quota_dict['description']
    priority = quota_dict['priority']
    
    # 验证字段类型
    if not isinstance(quota_code, str) or not quota_code.strip():
        raise ValueError(f"quota_code 必须是非空字符串")
    
    if not isinstance(quota_name, str) or not quota_name.strip():
        raise ValueError(f"quota_name 必须是非空字符串")
    
    if not isinstance(description, str):
        raise ValueError(f"description 必须是字符串")
    
    if not isinstance(priority, str):
        raise ValueError(f"priority 必须是字符串")
    
    # 验证 priority 值
    valid_priorities = ['high', 'medium', 'low', 'critical']
    if priority.lower() not in valid_priorities:
        raise ValueError(f"priority 必须是以下值之一: {', '.join(valid_priorities)}")
    
    # 解析 Cache TTL（可选，有默认值）
    cache_ttl_limit = quota_dict.get('cache_ttl_limit', 86400)  # 默认 24 小时
    cache_ttl_usage = quota_dict.get('cache_ttl_usage', 3600)   # 默认 1 小时
    
    # 验证 TTL 值
    if not isinstance(cache_ttl_limit, int) or cache_ttl_limit <= 0:
        raise ValueError(f"cache_ttl_limit 必须是正整数")
    if not isinstance(cache_ttl_usage, int) or cache_ttl_usage <= 0:
        raise ValueError(f"cache_ttl_usage 必须是正整数")
    
    return QuotaItem(
        quota_code=quota_code.strip(),
        quota_name=quota_name.strip(),
        description=description,
        priority=priority.lower(),
        cache_ttl_limit=cache_ttl_limit,
        cache_ttl_usage=cache_ttl_usage
    )


def print_quota_config(config: QuotaConfig):
    """
    打印配额配置结构（用于调试和验证）
    
    Args:
        config: QuotaConfig 对象
    """
    print("=" * 60)
    print("配额配置结构")
    print("=" * 60)
    
    # 打印 AWS 配额
    if config.aws:
        print(f"\n【AWS 配额】共 {len(config.aws)} 个服务")
        for service, service_config in config.aws.items():
            print(f"\n  服务: {service}")
            
            # 检查是否是 discovery 配置
            if isinstance(service_config, dict) and 'discovery' in service_config:
                discovery = service_config['discovery']
                print(f"    类型: Discovery（动态发现）")
                print(f"    匹配规则数量: {len(discovery.match_rules)}")
                for idx, rule in enumerate(discovery.match_rules):
                    keywords = rule.get('name_contains', [])
                    print(f"      规则 {idx+1}: 名称包含 {keywords}")
                print(f"    默认优先级: {discovery.default_priority}")
            else:
                # 声明型配置
                quotas = service_config
                print(f"    类型: 声明型")
                print(f"    配额数量: {len(quotas)}")
                for quota in quotas[:3]:  # 只显示前 3 个
                    print(f"      - {quota.quota_code}: {quota.quota_name} ({quota.priority})")
                if len(quotas) > 3:
                    print(f"      ... 还有 {len(quotas) - 3} 个配额")
    else:
        print("\n【AWS 配额】无")
    
    # 打印 Aliyun 配额
    if config.aliyun:
        print(f"\n【Aliyun 配额】共 {len(config.aliyun)} 个服务")
        for service, quotas in config.aliyun.items():
            print(f"\n  服务: {service}")
            print(f"    配额数量: {len(quotas)}")
            for quota in quotas[:3]:  # 只显示前 3 个
                print(f"      - {quota.quota_code}: {quota.quota_name} ({quota.priority})")
            if len(quotas) > 3:
                print(f"      ... 还有 {len(quotas) - 3} 个配额")
    else:
        print("\n【Aliyun 配额】无")
    
    print("\n" + "=" * 60)
