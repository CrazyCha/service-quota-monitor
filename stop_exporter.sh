#!/bin/bash
# 停止 Service Quota Exporter 脚本

echo "正在查找并停止 Service Quota Exporter 进程..."

# 查找所有相关的 Python 进程
PIDS=$(ps aux | grep -E "python.*main.py|quota.*exporter" | grep -v grep | awk '{print $2}')

if [ -z "$PIDS" ]; then
    echo "✓ 没有找到正在运行的 exporter 进程"
else
    echo "找到以下进程:"
    ps aux | grep -E "python.*main.py|quota.*exporter" | grep -v grep
    
    echo ""
    echo "正在停止进程..."
    for PID in $PIDS; do
        echo "  停止进程 $PID..."
        kill $PID 2>/dev/null
    done
    
    # 等待进程结束
    sleep 2
    
    # 检查是否还有进程在运行
    REMAINING=$(ps aux | grep -E "python.*main.py|quota.*exporter" | grep -v grep | awk '{print $2}')
    if [ -n "$REMAINING" ]; then
        echo "警告: 仍有进程未停止，强制终止..."
        for PID in $REMAINING; do
            kill -9 $PID 2>/dev/null
        done
    fi
    
    echo "✓ 所有进程已停止"
fi

# 检查并清理端口
echo ""
echo "检查端口占用情况..."

for PORT in 8000 8001; do
    PID=$(lsof -ti :$PORT 2>/dev/null)
    if [ -n "$PID" ]; then
        echo "  端口 $PORT 被进程 $PID 占用，正在清理..."
        kill -9 $PID 2>/dev/null
        sleep 1
        if lsof -ti :$PORT >/dev/null 2>&1; then
            echo "  警告: 端口 $PORT 仍被占用"
        else
            echo "  ✓ 端口 $PORT 已释放"
        fi
    else
        echo "  ✓ 端口 $PORT 未被占用"
    fi
done

echo ""
echo "完成！"






