Service Quota Exporter - 代码 Review 包
版本: 20251229
打包时间: Mon Dec 29 11:10:15 CST 2025

============================================================
项目概述
============================================================

这是一个 AWS Service Quota Exporter，用于收集和导出 AWS 服务配额数据。

核心功能：
- 从 CMDB MySQL 数据库自动发现和管理多个 AWS 账号
- 支持 EC2、EBS、ELB、EKS、ElastiCache、Route53、CloudFront、SageMaker 等 8 个服务
- 自动发现每个账号实际使用的 EC2 Region
- 采集配额 Limit 和 Usage 数据
- 通过 Prometheus 格式暴露指标

============================================================
代码结构
============================================================

主要目录：
- api/aws/          - AWS 服务 API 客户端封装
- provider/         - 账号/区域/凭证发现层
  - discovery/      - CMDB Provider 实现
  - aws/            - AWS 配额采集逻辑
- collector/        - Prometheus 指标收集器
- config/           - 配额配置文件
- cache/            - 缓存层（Limit 缓存、Usage 缓存）
- scheduler/        - 定时任务调度器
- cloudwatch/      - CloudWatch 集成
- quota/            - 配额匹配逻辑
- retry/            - 重试机制

主要文件：
- main.py           - 主程序入口
- requirements.txt  - Python 依赖
- README.md         - 详细文档（包含架构、成本分析）
- FOLDER_STRUCTURE.md - 文件夹结构说明

============================================================
技术栈
============================================================

- Python 3.x
- Flask - HTTP 服务器
- prometheus-client - Prometheus 指标
- boto3 - AWS SDK
- pymysql - MySQL 数据库连接（CMDB）

============================================================
关键设计
============================================================

1. Provider 模式：
   - 只支持 CMDB 模式（从 MySQL 数据库读取账号、区域、凭证）
   - 账号列表缓存 24 小时
   - EC2 Region 自动发现（只采集有 EC2 实例的 Region）
   - 凭证缓存 24 小时

2. 缓存策略：
   - Limit 缓存：文件缓存，24 小时 TTL（大幅减少 API 调用）
   - Usage 缓存：内存缓存，1 小时 TTL
   - 账号/区域缓存：文件缓存，24 小时 TTL

3. 并发采集：
   - 使用 ThreadPoolExecutor 并发处理多个账号
   - 默认 3 个并发线程
   - API 调用延迟 0.1 秒，减少限流风险

4. 定时任务：
   - Limit 采集：每 24 小时执行一次
   - Usage 采集：每 1 小时执行一次

============================================================
API 调用和成本
============================================================

1. Service Quotas API：免费
   - GetServiceQuota：获取配额 Limit
   - ListServiceQuotas：列出服务配额（SageMaker Discovery）

2. CloudWatch API：./package_for_review.sh.01 per 1,000 requests
   - 受缓存保护，每小时最多 1 次
   - 估算成本：约 .75/月

3. 其他 AWS API：免费
   - EC2, EBS, ELB, EKS, ElastiCache, Route53, CloudFront, SageMaker 的 Describe API

总成本估算：约 .75/月（29 个账号）

============================================================
Review 重点
============================================================

1. API 调用频率和限流处理
   - 代码位置：main.py 中的 _collect_account_region_quotas 函数
   - 限流处理：TooManyRequestsException 的指数退避重试

2. 缓存机制
   - Limit 缓存：cache/quota_limit_cache.py
   - Usage 缓存：provider/aws/usage_collector.py 中的 MemoryCache

3. 并发采集
   - 代码位置：main.py 中的 collect_quotas 函数
   - 使用 ThreadPoolExecutor，默认 3 个并发线程

4. 错误处理
   - 单个账号异常不影响其他账号
   - 详细的日志记录

5. 安全性
   - 凭证从 CMDB 数据库读取，不存储在代码中
   - 凭证缓存 24 小时

============================================================
联系方式
============================================================

如有问题，请联系开发团队。

