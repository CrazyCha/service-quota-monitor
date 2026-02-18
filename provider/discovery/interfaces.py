# -*- coding: utf-8 -*-
"""
Provider Discovery 接口定义

功能：
- 定义 AccountProvider 和 RegionProvider 接口
- 主流程只依赖接口，不关心具体实现
"""

from abc import ABC, abstractmethod
from typing import List


class AccountProvider(ABC):
    """
    账号发现接口
    
    功能：
    - 提供账号列表
    - 不涉及 AK/SK（凭证由其他模块管理）
    """
    
    @abstractmethod
    def get_accounts(self) -> List[str]:
        """
        获取账号列表
        
        Returns:
            账号 ID 列表
        """
        pass
    
    @abstractmethod
    def get_provider_type(self) -> str:
        """
        获取 Provider 类型（用于日志和标识）
        
        Returns:
            Provider 类型名称，如 "static", "cmdb"
        """
        pass


class RegionProvider(ABC):
    """
    区域发现接口
    
    功能：
    - 提供区域列表
    - 可以为每个账号提供不同的区域列表
    """
    
    @abstractmethod
    def get_regions(self, account_id: str = None) -> List[str]:
        """
        获取区域列表
        
        Args:
            account_id: 账号 ID（可选，某些实现可能需要）
        
        Returns:
            区域列表
        """
        pass
    
    @abstractmethod
    def get_provider_type(self) -> str:
        """
        获取 Provider 类型（用于日志和标识）
        
        Returns:
            Provider 类型名称，如 "static", "cmdb"
        """
        pass






