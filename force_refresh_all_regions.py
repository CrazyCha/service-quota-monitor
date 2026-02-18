#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
强制刷新所有账号的 EC2 Region 缓存

用于重新探测所有账号的 EC2 Region 使用情况
"""

import os
import sys

# 添加当前目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 设置环境变量
os.environ['PROVIDER_TYPE'] = 'cmdb'
os.environ['DB_PASSWORD'] = '25Y572zueyaO_H05N'

import logging
from provider.discovery.cmdb_provider import CMDBAccountProvider
from provider.discovery.active_region_discoverer import ActiveRegionDiscoverer

# 配置日志（显示 INFO 级别）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def force_refresh_all_regions():
    """强制刷新所有账号的 EC2 Region 缓存"""
    logger.info("=" * 60)
    logger.info("强制刷新所有账号的 EC2 Region 缓存")
    logger.info("=" * 60)
    
    try:
        # 初始化
        account_provider = CMDBAccountProvider()
        region_discoverer = ActiveRegionDiscoverer()
        
        # 强制刷新（忽略缓存）
        logger.info("开始强制刷新所有账号的 EC2 Region...")
        logger.info("注意：这将重新探测所有账号的所有 Region，可能需要几分钟")
        
        active_regions_map = region_discoverer.discover_ec2_used_regions_from_provider(
            account_provider,
            use_cache=False,  # 不使用缓存
            force_refresh=True  # 强制刷新
        )
        
        logger.info("=" * 60)
        logger.info("刷新完成！结果汇总：")
        logger.info("=" * 60)
        
        total_regions = 0
        accounts_with_regions = 0
        
        for account_id, regions in sorted(active_regions_map.items()):
            if regions:
                accounts_with_regions += 1
                total_regions += len(regions)
                logger.info(f"账号 {account_id}: {len(regions)} 个 EC2 Region - {regions}")
            else:
                logger.debug(f"账号 {account_id}: 0 个 EC2 Region")
        
        logger.info("=" * 60)
        logger.info(f"汇总：")
        logger.info(f"  - 总账号数: {len(active_regions_map)}")
        logger.info(f"  - 有 EC2 Region 的账号: {accounts_with_regions}")
        logger.info(f"  - 总 Region 数量: {total_regions}")
        logger.info(f"  - 平均每个账号: {total_regions / len(active_regions_map) if active_regions_map else 0:.1f} 个 Region")
        logger.info("=" * 60)
        
        return active_regions_map
        
    except Exception as e:
        logger.error(f"刷新失败: {e}", exc_info=True)
        return None

if __name__ == '__main__':
    force_refresh_all_regions()

