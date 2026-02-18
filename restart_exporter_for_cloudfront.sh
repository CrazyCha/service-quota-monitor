#!/bin/bash
# 重启 Exporter 以加载重构后的 CloudFront 代码

echo "============================================================"
echo "重启 Exporter（加载重构后的 CloudFront 代码）"
echo "============================================================"
echo ""

# 1. 停止当前 exporter
echo "=== 1. 停止当前 Exporter ==="
EXPORTER_PIDS=$(ps aux | grep -E "python.*main.py" | grep -v grep | awk '{print $2}')
if [ -n "$EXPORTER_PIDS" ]; then
    echo "找到 Exporter 进程: $EXPORTER_PIDS"
    for PID in $EXPORTER_PIDS; do
        echo "  停止进程 $PID..."
        kill $PID 2>/dev/null
    done
    sleep 2
    
    # 检查是否还有进程
    REMAINING=$(ps aux | grep -E "python.*main.py" | grep -v grep | awk '{print $2}')
    if [ -n "$REMAINING" ]; then
        echo "  强制停止剩余进程..."
        for PID in $REMAINING; do
            kill -9 $PID 2>/dev/null
        done
    fi
    echo "✅ Exporter 已停止"
else
    echo "⚠️  未找到运行中的 Exporter 进程"
fi
echo ""

# 2. 清理端口占用（如果需要）
echo "=== 2. 检查端口占用 ==="
PORT_8000=$(lsof -ti:8000 2>/dev/null)
if [ -n "$PORT_8000" ]; then
    echo "端口 8000 被占用，PID: $PORT_8000"
    echo "  清理端口占用..."
    kill -9 $PORT_8000 2>/dev/null
    sleep 1
    echo "✅ 端口已清理"
else
    echo "✅ 端口 8000 未被占用"
fi
echo ""

# 3. 检查环境变量
echo "=== 3. 检查环境变量 ==="
if [ -z "$PROVIDER_TYPE" ]; then
    export PROVIDER_TYPE=cmdb
    echo "✅ 设置 PROVIDER_TYPE=cmdb"
else
    echo "✅ PROVIDER_TYPE=$PROVIDER_TYPE"
fi

if [ -z "$DB_PASSWORD" ]; then
    echo "⚠️  DB_PASSWORD 未设置"
    echo "   请设置: export DB_PASSWORD='your_password'"
    echo "   或编辑此脚本添加密码"
    exit 1
else
    echo "✅ DB_PASSWORD 已设置"
fi
echo ""

# 4. 启动 exporter
echo "=== 4. 启动 Exporter ==="
echo "正在启动 exporter（后台运行）..."
cd "$(dirname "$0")"
nohup python3 main.py > exporter.log 2>&1 &
EXPORTER_PID=$!
echo "✅ Exporter 已启动，PID: $EXPORTER_PID"
echo ""

# 5. 等待 exporter 启动
echo "=== 5. 等待 Exporter 启动 ==="
for i in {1..10}; do
    sleep 2
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "✅ Exporter 启动成功（等待了 $((i * 2)) 秒）"
        break
    fi
    echo "   等待中... ($i/10)"
done

if ! curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "⚠️  Exporter 启动可能失败，请检查日志: tail -f exporter.log"
    exit 1
fi
echo ""

# 6. 提示等待 Usage 采集
echo "============================================================"
echo "重启完成"
echo "============================================================"
echo ""
echo "✅ Exporter 已重启并加载重构后的 CloudFront 代码"
echo ""
echo "📋 下一步："
echo "1. 等待 1-2 分钟让 Usage 采集完成"
echo "2. 检查 CloudFront Usage 指标："
echo "   curl -s http://localhost:8000/metrics | grep 'service=\"cloudfront\"' | grep 'cloud_service_quota_usage' | grep -v 'NaN'"
echo ""
echo "📊 查看日志："
echo "   tail -f exporter.log | grep -i cloudfront"
echo ""
echo "============================================================"

