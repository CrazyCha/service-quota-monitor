# -*- coding: utf-8 -*-
"""
AWS Provider 主实现模块

功能：
- 实现 Provider 接口
- 协调配额拉取和使用量收集
- 处理多 region 和多 service 的并发收集
"""

# TODO: 实现 AWS Provider
class AWSProvider:
    """
    AWS Provider 实现
    
    功能：
    - 获取 AWS Service Quotas API 的配额限制值
    - 通过 CloudWatch 或 API 获取使用量
    - 返回配额和使用量数据供 Prometheus 暴露
    """
    
    def __init__(self, config, quota_definitions, cloudwatch_dimensions):
        """
        初始化 AWS Provider
        
        Args:
            config: Provider 配置（account_id, access_key, secret_key, regions, services）
            quota_definitions: 配额定义对象
            cloudwatch_dimensions: CloudWatch 维度映射对象
        
        TODO:
            1. 初始化 AWS 凭证
            2. 创建 Service Quotas 客户端
            3. 创建 CloudWatch 客户端
            4. 创建各服务的 API 客户端（EC2, RDS, ELB 等）
            5. 初始化缓存和重试机制
        """
        pass
    
    def get_quotas(self, service, region):
        """
        获取指定服务和区域的配额限制值
        
        Args:
            service: 服务代码（如 'ec2', 'rds'）
            region: 区域（如 'us-east-1'）
        
        Returns:
            配额列表，每个配额包含 quota_code, quota_name, limit_value
        
        TODO:
            1. 从配额定义获取要监控的配额列表
            2. 对于 SageMaker，使用模糊匹配查找配额
            3. 调用 Service Quotas API 获取配额限制值
            4. 处理错误和重试
            5. 返回配额数据
        """
        pass
    
    def get_usage(self, service, region, quota_code):
        """
        获取指定配额的使用量
        
        Args:
            service: 服务代码
            region: 区域
            quota_code: 配额代码
        
        Returns:
            使用量值（float）
        
        TODO:
            1. 查询 CloudWatch 维度映射，判断使用 CloudWatch 还是 API
            2. 如果使用 CloudWatch：
               - 构建指标维度
               - 调用 CloudWatch API 获取指标值
            3. 如果使用 API：
               - 根据 api_method 调用对应的服务 API
               - 使用 calculator 计算使用量
            4. 处理缓存（如果配置了 cache_ttl）
            5. 处理错误和重试
            6. 返回使用量值
        """
        pass

