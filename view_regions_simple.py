#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单查看各账号的 Region 信息（不依赖 boto3）

功能：
1. 从缓存查看各账号的 EC2 Region
2. 从 metrics 端点查看实际采集的 region
"""

import os
import json
import time
import urllib.request
import re

def view_from_cache():
    """从缓存查看各账号的 EC2 Region"""
    print("=" * 60)
    print("方法 1: 从缓存查看各账号的 EC2 Region")
    print("=" * 60)
    
    cache_dir = os.getenv('EC2_REGIONS_CACHE_DIR', '.ec2_regions_cache')
    
    if not os.path.exists(cache_dir):
        print(f"⚠️  缓存目录不存在: {cache_dir}")
        print("   说明：还没有执行过 EC2 Region 发现")
        return {}
    
    cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.json')]
    
    if not cache_files:
        print(f"⚠️  缓存目录为空: {cache_dir}")
        print("   说明：还没有执行过 EC2 Region 发现")
        return {}
    
    print(f"找到 {len(cache_files)} 个账号的缓存文件\n")
    
    result = {}
    for cache_file in sorted(cache_files):
        account_id = cache_file.replace('.json', '')
        cache_path = os.path.join(cache_dir, cache_file)
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            regions = cache_data.get('regions', [])
            timestamp = cache_data.get('timestamp', 0)
            
            # 检查缓存是否过期（24h）
            cache_age = time.time() - timestamp
            cache_age_hours = cache_age / 3600
            is_expired = cache_age > 86400
            
            status = "✅ 有效" if not is_expired else "⚠️  已过期"
            
            result[account_id] = regions
            
            print(f"账号 {account_id}: {len(regions)} 个 EC2 Region {status} (缓存年龄: {cache_age_hours:.1f} 小时)")
            if regions:
                print(f"  Region 列表: {regions}")
            else:
                print(f"  Region 列表: [] (该账号没有使用过 EC2)")
            print()
            
        except Exception as e:
            print(f"❌ 读取缓存文件 {cache_file} 失败: {e}")
    
    return result


def view_from_metrics():
    """从 metrics 端点查看实际采集的 region"""
    print("=" * 60)
    print("方法 2: 从 Metrics 端点查看实际采集的 Region")
    print("=" * 60)
    
    # 检查 exporter 是否运行
    try:
        response = urllib.request.urlopen('http://localhost:8000/health', timeout=5)
        print("✅ Exporter 正在运行\n")
    except Exception as e:
        print(f"❌ Exporter 未运行或无法访问: {e}")
        print("   请先启动 exporter: python3 main.py")
        return {}
    
    # 获取 metrics
    try:
        response = urllib.request.urlopen('http://localhost:8000/metrics', timeout=10)
        metrics_text = response.read().decode()
        
        # 提取各账号的 region
        account_regions = {}
        
        for line in metrics_text.split('\n'):
            if 'cloud_service_quota_limit' in line and 'account_id=' in line:
                # 提取 account_id 和 region
                account_match = re.search(r'account_id="([^"]+)"', line)
                region_match = re.search(r'region="([^"]+)"', line)
                
                if account_match and region_match:
                    account_id = account_match.group(1)
                    region = region_match.group(1)
                    
                    if account_id not in account_regions:
                        account_regions[account_id] = set()
                    account_regions[account_id].add(region)
        
        if account_regions:
            print(f"找到 {len(account_regions)} 个账号的采集数据\n")
            
            for account_id, regions in sorted(account_regions.items()):
                regions_list = sorted(list(regions))
                print(f"账号 {account_id}: {len(regions_list)} 个 Region")
                print(f"  Region 列表: {regions_list}")
                print()
            
            return {k: list(v) for k, v in account_regions.items()}
        else:
            print("⚠️  未找到任何账号的采集数据")
            return {}
            
    except Exception as e:
        print(f"❌ 读取 metrics 失败: {e}")
        return {}


def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("查看各账号的 Region 信息")
    print("=" * 60)
    print()
    
    # 方法 1: 从缓存查看
    cache_result = view_from_cache()
    
    # 方法 2: 从 metrics 查看
    print()
    metrics_result = view_from_metrics()
    
    # 汇总信息
    print("\n" + "=" * 60)
    print("汇总信息")
    print("=" * 60)
    
    if cache_result:
        print(f"\n从缓存读取: {len(cache_result)} 个账号有 EC2 Region 缓存")
        total_regions = sum(len(regions) for regions in cache_result.values())
        avg_regions = total_regions / len(cache_result) if cache_result else 0
        print(f"总 Region 数量: {total_regions}")
        print(f"平均每个账号: {avg_regions:.1f} 个 EC2 Region")
        
        # 统计 Region 分布
        region_count_dist = {}
        for regions in cache_result.values():
            count = len(regions)
            region_count_dist[count] = region_count_dist.get(count, 0) + 1
        
        print("\nRegion 数量分布:")
        for count, num_accounts in sorted(region_count_dist.items()):
            print(f"  {num_accounts} 个账号有 {count} 个 EC2 Region")
    
    if metrics_result:
        print(f"\n从 Metrics 读取: {len(metrics_result)} 个账号有采集数据")
        total_regions = sum(len(regions) for regions in metrics_result.values())
        avg_regions = total_regions / len(metrics_result) if metrics_result else 0
        print(f"总 Region 数量: {total_regions}")
        print(f"平均每个账号: {avg_regions:.1f} 个 Region")
    
    print("\n" + "=" * 60)
    print("完成")
    print("=" * 60)
    
    # 提示
    if not cache_result and not metrics_result:
        print("\n💡 提示:")
        print("   如果缓存为空，可以运行: python3 discover_active_regions.py")
        print("   如果 exporter 未运行，可以启动: python3 main.py")


if __name__ == '__main__':
    main()

