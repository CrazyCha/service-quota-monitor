#!/bin/bash
# 启动 CMDB 模式的 Exporter

echo "============================================================"
echo "启动 Service Quota Exporter (CMDB 模式)"
echo "============================================================"

# 检查环境变量
if [ -z "$DB_PASSWORD" ]; then
    echo "❌ 错误: 环境变量 DB_PASSWORD 未设置"
    echo "   请运行: export DB_PASSWORD='your_password'"
    exit 1
fi

# 显示配置信息
echo ""
echo "配置信息:"
echo "  DB_PASSWORD: ${DB_PASSWORD:+已设置}"
echo "  CMDB_REGIONS: ${CMDB_REGIONS:-未设置（将使用默认值）}"
echo ""

# 进入项目目录
cd /Users/wendy/aws-service-quota-exporter/service-quota-exporter

# 检查 Python 和依赖
echo "检查依赖..."
if ! python3 -c "import pymysql" 2>/dev/null; then
    echo "❌ pymysql 未安装"
    echo "   请运行: pip3 install pymysql"
    exit 1
fi

echo "✅ 依赖检查通过"
echo ""

# 启动 exporter
echo "启动 exporter..."
echo "============================================================"
python3 main.py

