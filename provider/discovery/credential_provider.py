# -*- coding: utf-8 -*-
"""
Credential Provider 实现

功能：
- 管理 AWS 账号的 Access Key 和 Secret Key
- 支持从 CMDB 数据库读取凭证
- 提供凭证缓存（1小时 TTL）
"""

import logging
import os
import time
from typing import Dict, Optional, Tuple
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class CredentialProvider(ABC):
    """
    凭证 Provider 接口
    
    功能：
    - 提供账号的 Access Key 和 Secret Key
    - 支持缓存机制
    """
    
    @abstractmethod
    def get_credentials(self, account_id: str) -> Optional[Dict[str, str]]:
        """
        获取账号的凭证
        
        Args:
            account_id: 账号 ID
        
        Returns:
            凭证字典，包含 'access_key' 和 'secret_key'，如果不存在返回 None
        """
        pass
    
    @abstractmethod
    def get_provider_type(self) -> str:
        """
        获取 Provider 类型
        
        Returns:
            Provider 类型名称，如 "cmdb", "static"
        """
        pass


class CMDBCredentialProvider(CredentialProvider):
    """
    CMDB 凭证 Provider（从 MySQL 数据库读取）
    
    功能：
    - 从 CMDBAccountProvider 获取账号凭证
    - 提供凭证缓存（1小时 TTL）
    - 单个账号异常不影响其他账号
    """
    
    def __init__(self, cmdb_account_provider):
        """
        初始化 CMDB Credential Provider
        
        Args:
            cmdb_account_provider: CMDBAccountProvider 实例
        """
        self.cmdb_account_provider = cmdb_account_provider
        self._cache: Dict[str, Tuple[Dict[str, str], float]] = {}  # account_id -> (credentials, expiration_time)
        self.cache_ttl = 3600  # 1 小时缓存
        logger.info("初始化 CMDB Credential Provider（带缓存）")
    
    def get_credentials(self, account_id: str) -> Optional[Dict[str, str]]:
        """
        获取账号的凭证（带缓存）
        
        Args:
            account_id: 账号 ID
        
        Returns:
            凭证字典，包含 'access_key' 和 'secret_key'，如果不存在返回 None
        """
        # 检查缓存
        if account_id in self._cache:
            credentials, expiration_time = self._cache[account_id]
            if time.time() < expiration_time:
                logger.debug(f"凭证缓存命中: account_id={account_id}")
                return credentials.copy()
            else:
                # 缓存过期，删除
                del self._cache[account_id]
        
        # 缓存未命中或过期，从数据库读取
        try:
            all_credentials = self.cmdb_account_provider.get_account_credentials()
            credentials = all_credentials.get(account_id)
            
            if credentials:
                # 缓存凭证
                expiration_time = time.time() + self.cache_ttl
                self._cache[account_id] = (credentials.copy(), expiration_time)
                logger.debug(f"从 CMDB 数据库读取凭证并缓存: account_id={account_id}")
                return credentials.copy()
            else:
                logger.warning(f"账号 {account_id} 的凭证在 CMDB 数据库中不存在")
                return None
                
        except Exception as e:
            logger.error(f"获取账号 {account_id} 的凭证失败: {e}", exc_info=True)
            # 单个账号异常不影响其他账号，返回 None
            return None
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "cmdb"
    
    def clear_cache(self, account_id: str = None):
        """
        清除缓存
        
        Args:
            account_id: 账号 ID，如果为 None 则清除所有缓存
        """
        if account_id:
            if account_id in self._cache:
                del self._cache[account_id]
                logger.debug(f"清除账号 {account_id} 的凭证缓存")
        else:
            self._cache.clear()
            logger.debug("清除所有凭证缓存")

