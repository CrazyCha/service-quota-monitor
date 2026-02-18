# -*- coding: utf-8 -*-
"""
缓存实现模块

功能：
- 内存缓存实现（带 TTL）
- 支持缓存命中率统计
"""

import time
import threading
from typing import Optional, Tuple, Dict, Any


class MemoryCache:
    """
    内存缓存实现
    
    功能：
    - 存储 API 响应数据
    - 支持 TTL（Time To Live）
    - 自动清理过期条目
    """
    
    def __init__(self):
        """初始化内存缓存"""
        self._cache: Dict[str, Tuple[Any, float]] = {}  # key -> (value, expiration_time)
        self._lock = threading.RLock()  # 线程安全锁
    
    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        """
        获取缓存值
        
        Args:
            key: 缓存键
        
        Returns:
            (value, exists) 元组，exists=True 表示缓存命中且未过期
        """
        with self._lock:
            if key not in self._cache:
                return None, False
            
            value, expiration_time = self._cache[key]
            
            # 检查是否过期
            if time.time() > expiration_time:
                # 过期，删除并返回未命中
                del self._cache[key]
                return None, False
            
            # 缓存命中
            return value, True
    
    def set(self, key: str, value: Any, ttl: int):
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
            ttl: 生存时间（秒）
        """
        with self._lock:
            expiration_time = time.time() + ttl
            self._cache[key] = (value, expiration_time)
    
    def delete(self, key: str):
        """删除缓存值"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
    
    def clear(self):
        """清空所有缓存"""
        with self._lock:
            self._cache.clear()
    
    def cleanup_expired(self):
        """清理过期条目（可选，用于定期清理）"""
        current_time = time.time()
        with self._lock:
            expired_keys = [
                key for key, (_, expiration_time) in self._cache.items()
                if current_time > expiration_time
            ]
            for key in expired_keys:
                del self._cache[key]
