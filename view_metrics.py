#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查看采集到的 Quota 指标

功能：
1. 从 metrics 端点获取所有指标
2. 按账号、服务、Region 统计
3. 显示特定账号的指标
"""

import urllib.request
import sys
import re
from collections import defaultdict

def fetch_metrics():
    """从 metrics 端点获取指标"""
    try:
        response = urllib.request.urlopen('http://localhost:8000/metrics', timeout=10)
        return response.read().decode('utf-8')
    except Exception as e:
        print(f"❌ 无法连接到 exporter: {e}")
        print("   请确保 exporter 正在运行: python3 main.py")
        return None

def parse_metrics(metrics_text):
    """解析 metrics 文本"""
    metrics = {
        'limit': [],
        'usage': [],
        'usage_percent': []
    }
    
    for line in metrics_text.split('\n'):
        if line.startswith('#') or not line.strip():
            continue
        
        if 'cloud_service_quota_limit' in line:
            metrics['limit'].append(line)
        elif 'cloud_service_quota_usage' in line:
            metrics['usage'].append(line)
        elif 'cloud_quota_usage_percent' in line:
            metrics['usage_percent'].append(line)
    
    return metrics

def extract_labels(metric_line):
    """从 metric 行中提取标签"""
    # 格式: metric_name{label1="value1",label2="value2"} value
    match = re.search(r'\{([^}]+)\}', metric_line)
    if not match:
        return {}
    
    labels_str = match.group(1)
    labels = {}
    
    # 解析标签
    for label_match in re.finditer(r'(\w+)="([^"]+)"', labels_str):
        key, value = label_match.groups()
        labels[key] = value
    
    # 提取值
    value_match = re.search(r'\}\s+([\d.]+)', metric_line)
    if value_match:
        labels['value'] = float(value_match.group(1))
    
    return labels

def view_summary():
    """查看汇总信息"""
    print("=" * 60)
    print("Quota 指标汇总")
    print("=" * 60)
    
    metrics_text = fetch_metrics()
    if not metrics_text:
        return
    
    metrics = parse_metrics(metrics_text)
    
    # 统计
    stats = {
        'accounts': set(),
        'services': set(),
        'regions': set(),
        'quotas': set()
    }
    
    for metric_type, lines in metrics.items():
        for line in lines:
            labels = extract_labels(line)
            if 'account_id' in labels:
                stats['accounts'].add(labels['account_id'])
            if 'service' in labels:
                stats['services'].add(labels['service'])
            if 'region' in labels:
                stats['regions'].add(labels['region'])
            if 'quota_code' in labels:
                stats['quotas'].add(labels['quota_code'])
    
    print(f"\n总指标数:")
    print(f"  - Limit: {len(metrics['limit'])} 条")
    print(f"  - Usage: {len(metrics['usage'])} 条")
    print(f"  - Usage Percent: {len(metrics['usage_percent'])} 条")
    
    print(f"\n覆盖范围:")
    print(f"  - 账号数: {len(stats['accounts'])}")
    print(f"  - 服务数: {len(stats['services'])}")
    print(f"  - Region 数: {len(stats['regions'])}")
    print(f"  - Quota 数: {len(stats['quotas'])}")
    
    if stats['accounts']:
        print(f"\n账号列表: {sorted(stats['accounts'])}")
    if stats['services']:
        print(f"\n服务列表: {sorted(stats['services'])}")
    if stats['regions']:
        print(f"\nRegion 列表: {sorted(stats['regions'])}")

def view_by_account(account_id=None):
    """按账号查看指标"""
    print("=" * 60)
    if account_id:
        print(f"账号 {account_id} 的 Quota 指标")
    else:
        print("按账号查看 Quota 指标")
    print("=" * 60)
    
    metrics_text = fetch_metrics()
    if not metrics_text:
        return
    
    metrics = parse_metrics(metrics_text)
    
    # 按账号分组
    by_account = defaultdict(lambda: {
        'limit': [],
        'usage': [],
        'usage_percent': []
    })
    
    for metric_type, lines in metrics.items():
        for line in lines:
            labels = extract_labels(line)
            if 'account_id' in labels:
                acc_id = labels['account_id']
                if not account_id or acc_id == account_id:
                    by_account[acc_id][metric_type].append(labels)
    
    for acc_id, data in sorted(by_account.items()):
        print(f"\n账号: {acc_id}")
        print(f"  - Limit: {len(data['limit'])} 条")
        print(f"  - Usage: {len(data['usage'])} 条")
        print(f"  - Usage Percent: {len(data['usage_percent'])} 条")
        
        # 显示 Region 分布
        regions = set()
        for item in data['limit'] + data['usage']:
            if 'region' in item:
                regions.add(item['region'])
        if regions:
            print(f"  - Region: {sorted(regions)}")
        
        # 显示服务分布
        services = set()
        for item in data['limit'] + data['usage']:
            if 'service' in item:
                services.add(item['service'])
        if services:
            print(f"  - 服务: {sorted(services)}")

def view_by_service(service=None):
    """按服务查看指标"""
    print("=" * 60)
    if service:
        print(f"服务 {service} 的 Quota 指标")
    else:
        print("按服务查看 Quota 指标")
    print("=" * 60)
    
    metrics_text = fetch_metrics()
    if not metrics_text:
        return
    
    metrics = parse_metrics(metrics_text)
    
    # 按服务分组
    by_service = defaultdict(lambda: {
        'limit': [],
        'usage': [],
        'usage_percent': []
    })
    
    for metric_type, lines in metrics.items():
        for line in lines:
            labels = extract_labels(line)
            if 'service' in labels:
                svc = labels['service']
                if not service or svc == service:
                    by_service[svc][metric_type].append(labels)
    
    for svc, data in sorted(by_service.items()):
        print(f"\n服务: {svc}")
        print(f"  - Limit: {len(data['limit'])} 条")
        print(f"  - Usage: {len(data['usage'])} 条")
        print(f"  - Usage Percent: {len(data['usage_percent'])} 条")
        
        # 显示账号分布
        accounts = set()
        for item in data['limit'] + data['usage']:
            if 'account_id' in item:
                accounts.add(item['account_id'])
        if accounts:
            print(f"  - 账号数: {len(accounts)}")

def view_details(account_id=None, service=None, region=None):
    """查看详细指标"""
    print("=" * 60)
    print("Quota 指标详情")
    if account_id:
        print(f"账号: {account_id}")
    if service:
        print(f"服务: {service}")
    if region:
        print(f"Region: {region}")
    print("=" * 60)
    
    metrics_text = fetch_metrics()
    if not metrics_text:
        return
    
    metrics = parse_metrics(metrics_text)
    
    # 过滤和显示
    count = 0
    for metric_type, lines in metrics.items():
        for line in lines:
            labels = extract_labels(line)
            
            # 过滤条件
            if account_id and labels.get('account_id') != account_id:
                continue
            if service and labels.get('service') != service:
                continue
            if region and labels.get('region') != region:
                continue
            
            # 显示
            metric_name = line.split('{')[0]
            print(f"\n{metric_name}")
            print(f"  账号: {labels.get('account_id', 'N/A')}")
            print(f"  服务: {labels.get('service', 'N/A')}")
            print(f"  Region: {labels.get('region', 'N/A')}")
            print(f"  Quota Code: {labels.get('quota_code', 'N/A')}")
            print(f"  Quota Name: {labels.get('quota_name', 'N/A')}")
            print(f"  值: {labels.get('value', 'N/A')}")
            
            count += 1
            if count >= 20:  # 限制显示数量
                print("\n... (仅显示前 20 条)")
                return

def main():
    """主函数"""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'summary':
            view_summary()
        elif command == 'account':
            account_id = sys.argv[2] if len(sys.argv) > 2 else None
            view_by_account(account_id)
        elif command == 'service':
            service = sys.argv[2] if len(sys.argv) > 2 else None
            view_by_service(service)
        elif command == 'details':
            account_id = sys.argv[2] if len(sys.argv) > 2 else None
            service = sys.argv[3] if len(sys.argv) > 3 else None
            region = sys.argv[4] if len(sys.argv) > 4 else None
            view_details(account_id, service, region)
        else:
            print("用法:")
            print("  python3 view_metrics.py summary              # 查看汇总")
            print("  python3 view_metrics.py account [账号ID]     # 按账号查看")
            print("  python3 view_metrics.py service [服务名]     # 按服务查看")
            print("  python3 view_metrics.py details [账号ID] [服务名] [Region]  # 查看详情")
    else:
        # 默认显示汇总
        view_summary()
        print("\n" + "=" * 60)
        print("提示：使用以下命令查看更多信息")
        print("=" * 60)
        print("  python3 view_metrics.py summary")
        print("  python3 view_metrics.py account 955634015615")
        print("  python3 view_metrics.py service ec2")
        print("  python3 view_metrics.py details 955634015615 ec2 ap-southeast-1")

if __name__ == '__main__':
    main()

