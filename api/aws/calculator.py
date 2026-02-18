# -*- coding: utf-8 -*-
"""
使用量计算模块

功能：
- 对 API 返回的资源数据进行计算
- 支持多种计算类型（count, sum_size, sum_iops, max 等）
- 支持分组统计（按 VPC、AZ 等）
"""

# TODO: 实现使用量计算逻辑
class Calculator:
    """
    使用量计算器
    
    功能：
    - 根据计算类型对资源数据进行计算
    - 返回使用量值供 Prometheus 暴露
    """
    
    def calculate(self, calculation_type, data, params=None):
        """
        执行计算
        
        Args:
            calculation_type: 计算类型（'count', 'sum_size', 'sum_iops', 'max_rules_per_group' 等）
            data: 资源数据（API 返回的列表）
            params: 额外参数（如 volume_type, group_by 等）
        
        Returns:
            使用量值（float）
        
        TODO:
            实现各种计算类型：
            1. count: 计数
            2. sum_size: 求和（存储容量，GiB）
            3. sum_iops: 求和（IOPS）
            4. max_rules_per_group: 每组最大规则数
            5. count_cidr_blocks: CIDR 块计数
            6. max_security_groups_per_eni: 每个 ENI 的最大安全组数
            7. max_rules_per_alb: 每个 ALB 的最大规则数
            8. sum_allocated_storage: RDS 总存储容量
            9. 等等...
        """
        pass
    
    def group_by(self, data, field):
        """
        按字段分组
        
        Args:
            data: 资源数据列表
            field: 分组字段（如 'vpc-id', 'availability-zone'）
        
        Returns:
            分组后的数据字典
        
        TODO:
            1. 遍历数据列表
            2. 按指定字段分组
            3. 返回分组字典
        """
        pass

