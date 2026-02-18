#!/bin/bash
# 重启 Service Quota Exporter 脚本

echo "=== 重启 Service Quota Exporter ==="
echo ""

# 步骤 1: 停止当前运行的 exporter
echo "1. 停止当前运行的 exporter..."
cd /Users/wendy/aws-service-quota-exporter/service-quota-exporter

if [ -f "./stop_exporter.sh" ]; then
    ./stop_exporter.sh
else
    echo "  使用手动停止方式..."
    PIDS=$(ps aux | grep -E "python.*main.py" | grep -v grep | awk '{print $2}')
    if [ -n "$PIDS" ]; then
        for PID in $PIDS; do
            kill $PID 2>/dev/null
        done
        sleep 2
        # 强制停止未响应的进程
        REMAINING=$(ps aux | grep -E "python.*main.py" | grep -v grep | awk '{print $2}')
        if [ -n "$REMAINING" ]; then
            for PID in $REMAINING; do
                kill -9 $PID 2>/dev/null
            done
        fi
    fi
    # 清理端口
    lsof -ti :8000 | xargs kill -9 2>/dev/null
    lsof -ti :8001 | xargs kill -9 2>/dev/null
fi

echo ""
echo "2. 等待进程完全停止..."
sleep 2

echo ""
echo "3. 检查环境变量..."

# 使用 CMDB 模式（从 MySQL 数据库读取账号和区域）
echo "  使用 CMDB 模式"
if [ -z "$DB_PASSWORD" ]; then
    echo "  ❌ 错误: DB_PASSWORD 环境变量未设置"
    echo "     请运行: export DB_PASSWORD='your_password'"
    echo "     然后重新运行此脚本"
    exit 1
fi
echo "  ✓ DB_PASSWORD 已设置"

echo ""
echo "4. 启动 exporter..."
cd /Users/wendy/aws-service-quota-exporter/service-quota-exporter
nohup python3 main.py > exporter_runtime.log 2>&1 &

# 等待启动
sleep 5

echo ""
echo "5. 检查 exporter 状态..."
if ps aux | grep -E "python.*main.py" | grep -v grep > /dev/null; then
    echo "✓ Exporter 已成功启动"
    echo ""
    echo "6. 验证 metrics 端点..."
    if curl -s http://localhost:8000/metrics > /dev/null 2>&1; then
        echo "✓ Metrics 端点可访问"
        echo ""
        echo "=== 重启完成 ==="
        echo ""
        echo "Exporter 运行在: http://localhost:8000"
        echo "Metrics 端点: http://localhost:8000/metrics"
        echo "健康检查: http://localhost:8000/health"
    else
        echo "⚠ 警告: Metrics 端点暂时不可访问，请稍等片刻后重试"
    fi
else
    echo "✗ Exporter 启动失败，请查看日志"
fi

