# -*- coding: utf-8 -*-
"""
配额 Limit 缓存模块

功能：
- 文件缓存配额 Limit 数据（24 小时）
- 减少 API 调用，大幅提升采集速度
"""

import os
import json
import time
import logging
from typing import Optional, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class QuotaLimitCache:
    """
    配额 Limit 文件缓存
    
    功能：
    - 缓存配额 Limit 数据（24 小时）
    - 缓存键格式：{account_id}:{region}:{service}:{quota_code}
    - 缓存文件：.quota_limit_cache/{account_id}/{region}/{service}.json
    """
    
    def __init__(self, cache_dir: str = None, cache_ttl: int = None):
        """
        初始化配额 Limit 缓存
        
        Args:
            cache_dir: 缓存目录（默认：.quota_limit_cache）
            cache_ttl: 缓存时间（秒，默认：86400，即 24 小时）
        """
        self.cache_dir = cache_dir or os.getenv('QUOTA_LIMIT_CACHE_DIR', '.quota_limit_cache')
        self.cache_ttl = cache_ttl or int(os.getenv('QUOTA_LIMIT_CACHE_TTL', '86400'))  # 默认 24 小时
        
        # 创建缓存目录
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.info(f"初始化配额 Limit 缓存: {self.cache_dir}, TTL: {self.cache_ttl} 秒 ({self.cache_ttl // 3600} 小时)")
    
    def _get_cache_file_path(self, account_id: str, region: str, service: str) -> str:
        """获取缓存文件路径"""
        cache_subdir = os.path.join(self.cache_dir, account_id, region)
        return os.path.join(cache_subdir, f"{service}.json")
    
    def get(self, account_id: str, region: str, service: str, quota_code: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存的配额 Limit 数据
        
        Args:
            account_id: 账号 ID
            region: 区域
            service: 服务代码
            quota_code: 配额代码
        
        Returns:
            配额 Limit 数据（如果缓存有效），否则返回 None
        """
        cache_file = self._get_cache_file_path(account_id, region, service)
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查缓存是否过期
            cache_time = cache_data.get('timestamp', 0)
            if time.time() - cache_time > self.cache_ttl:
                logger.debug(f"配额 Limit 缓存已过期: {account_id}:{region}:{service}")
                return None
            
            # 获取指定配额的数据
            quotas = cache_data.get('quotas', {})
            quota_data = quotas.get(quota_code)
            
            if quota_data:
                logger.debug(f"从缓存获取配额 Limit: {account_id}:{region}:{service}:{quota_code}")
                return quota_data
            
            return None
            
        except Exception as e:
            logger.warning(f"读取配额 Limit 缓存失败: {cache_file}, 错误: {e}")
            return None
    
    def set(self, account_id: str, region: str, service: str, quota_code: str, quota_data: Dict[str, Any]):
        """
        设置缓存的配额 Limit 数据
        
        Args:
            account_id: 账号 ID
            region: 区域
            service: 服务代码
            quota_code: 配额代码
            quota_data: 配额 Limit 数据
        """
        cache_file = self._get_cache_file_path(account_id, region, service)
        cache_subdir = os.path.dirname(cache_file)
        
        # 创建缓存子目录
        if not os.path.exists(cache_subdir):
            os.makedirs(cache_subdir, exist_ok=True)
        
        try:
            # 读取现有缓存（如果存在）
            cache_data = {}
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                except:
                    pass
            
            # 更新缓存数据
            if 'quotas' not in cache_data:
                cache_data['quotas'] = {}
            
            cache_data['timestamp'] = time.time()
            cache_data['account_id'] = account_id
            cache_data['region'] = region
            cache_data['service'] = service
            cache_data['quotas'][quota_code] = quota_data
            
            # 保存到文件
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"已缓存配额 Limit: {account_id}:{region}:{service}:{quota_code}")
            
        except Exception as e:
            logger.warning(f"保存配额 Limit 缓存失败: {cache_file}, 错误: {e}")
    
    def clear(self, account_id: str = None, region: str = None, service: str = None):
        """
        清除缓存
        
        Args:
            account_id: 账号 ID（如果指定，只清除该账号的缓存）
            region: 区域（如果指定，只清除该区域的缓存）
            service: 服务（如果指定，只清除该服务的缓存）
        """
        if account_id and region and service:
            # 清除特定服务的缓存
            cache_file = self._get_cache_file_path(account_id, region, service)
            if os.path.exists(cache_file):
                os.remove(cache_file)
                logger.info(f"已清除缓存: {account_id}:{region}:{service}")
        elif account_id and region:
            # 清除特定区域的缓存
            cache_subdir = os.path.join(self.cache_dir, account_id, region)
            if os.path.exists(cache_subdir):
                import shutil
                shutil.rmtree(cache_subdir)
                logger.info(f"已清除缓存: {account_id}:{region}")
        elif account_id:
            # 清除特定账号的缓存
            cache_subdir = os.path.join(self.cache_dir, account_id)
            if os.path.exists(cache_subdir):
                import shutil
                shutil.rmtree(cache_subdir)
                logger.info(f"已清除缓存: {account_id}")
        else:
            # 清除所有缓存
            if os.path.exists(self.cache_dir):
                import shutil
                shutil.rmtree(self.cache_dir)
                os.makedirs(self.cache_dir, exist_ok=True)
                logger.info(f"已清除所有配额 Limit 缓存")
    
    def is_force_refresh(self) -> bool:
        """检查是否强制刷新缓存"""
        return os.getenv('FORCE_REFRESH_QUOTA_LIMITS', 'false').lower() == 'true'


