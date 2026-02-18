# -*- coding: utf-8 -*-
"""
定时任务实现模块

功能：
- 定时调用采集函数刷新数据
- 不直接操作 Prometheus metrics
- 不关心账号、region、service 细节
- 只负责"什么时候刷新"
"""

import threading
import time
import logging
from typing import Callable

logger = logging.getLogger(__name__)


class QuotaScheduler:
    """
    配额采集定时任务调度器
    
    职责：
    1. 定时调用"已有的采集函数"
    2. 不直接操作 Prometheus metrics
    3. 不关心账号、region、service 细节
    4. 只负责"什么时候刷新"
    """
    
    def __init__(
        self,
        collect_limit_func: Callable,
        collect_usage_func: Callable,
        limit_interval: int = 86400,  # 24 小时
        usage_interval: int = 3600     # 1 小时
    ):
        """
        初始化定时任务调度器
        
        Args:
            collect_limit_func: 采集 Limit 的函数
            collect_usage_func: 采集 Usage 的函数
            limit_interval: Limit 刷新间隔（秒），默认 86400（24 小时）
            usage_interval: Usage 刷新间隔（秒），默认 3600（1 小时）
        """
        self.collect_limit_func = collect_limit_func
        self.collect_usage_func = collect_usage_func
        self.limit_interval = limit_interval
        self.usage_interval = usage_interval
        
        # 控制标志
        self._running = False
        self._limit_thread: Optional[threading.Thread] = None
        self._usage_thread: Optional[threading.Thread] = None
        
        logger.info(f"QuotaScheduler 初始化完成: limit_interval={limit_interval}s, usage_interval={usage_interval}s")
    
    def start(self):
        """
        启动定时任务
        
        启动两个后台线程：
        - limit_refresh_loop: 每 limit_interval 秒执行一次
        - usage_refresh_loop: 每 usage_interval 秒执行一次
        """
        if self._running:
            logger.warning("定时任务已在运行")
            return
        
        self._running = True
        
        # 启动 Limit 刷新线程
        self._limit_thread = threading.Thread(
            target=self._limit_refresh_loop,
            name="LimitRefreshThread",
            daemon=True
        )
        self._limit_thread.start()
        logger.info("Limit 刷新线程已启动")
        
        # 启动 Usage 刷新线程
        self._usage_thread = threading.Thread(
            target=self._usage_refresh_loop,
            name="UsageRefreshThread",
            daemon=True
        )
        self._usage_thread.start()
        logger.info("Usage 刷新线程已启动")
        
        logger.info("定时任务调度器已启动")
    
    def stop(self):
        """
        停止定时任务
        """
        if not self._running:
            return
        
        self._running = False
        logger.info("停止定时任务调度器...")
        
        # 等待线程结束（最多等待 5 秒）
        if self._limit_thread and self._limit_thread.is_alive():
            self._limit_thread.join(timeout=5)
        
        if self._usage_thread and self._usage_thread.is_alive():
            self._usage_thread.join(timeout=5)
        
        logger.info("定时任务调度器已停止")
    
    def _limit_refresh_loop(self):
        """
        Limit 刷新循环
        
        每 limit_interval 秒执行一次 collect_limit_func
        """
        logger.info(f"[Scheduler] Limit 刷新循环启动，间隔: {self.limit_interval} 秒")
        
        while self._running:
            try:
                # 等待指定间隔
                time.sleep(self.limit_interval)
                
                if not self._running:
                    break
                
                # 执行 Limit 采集
                logger.info("[Scheduler] limit refresh triggered")
                self.collect_limit_func()
                logger.info("[Scheduler] limit refresh completed")
                
            except Exception as e:
                # 捕获异常，打印日志，不退出线程
                logger.error(f"[Scheduler] Limit 刷新异常: {e}", exc_info=True)
                # 继续循环，不退出
        
        logger.info("[Scheduler] Limit 刷新循环已退出")
    
    def _usage_refresh_loop(self):
        """
        Usage 刷新循环
        
        每 usage_interval 秒执行一次 collect_usage_func
        """
        logger.info(f"[Scheduler] Usage 刷新循环启动，间隔: {self.usage_interval} 秒")
        
        while self._running:
            try:
                # 等待指定间隔
                time.sleep(self.usage_interval)
                
                if not self._running:
                    break
                
                # 执行 Usage 采集
                logger.info("[Scheduler] usage refresh triggered")
                self.collect_usage_func()
                logger.info("[Scheduler] usage refresh completed")
                
            except Exception as e:
                # 捕获异常，打印日志，不退出线程
                logger.error(f"[Scheduler] Usage 刷新异常: {e}", exc_info=True)
                # 继续循环，不退出
        
        logger.info("[Scheduler] Usage 刷新循环已退出")
    
    def get_status(self) -> dict:
        """
        获取定时任务状态
        
        Returns:
            状态信息字典
        """
        return {
            'running': self._running,
            'limit_interval': self.limit_interval,
            'usage_interval': self.usage_interval,
            'limit_thread_alive': self._limit_thread.is_alive() if self._limit_thread else False,
            'usage_thread_alive': self._usage_thread.is_alive() if self._usage_thread else False
        }
