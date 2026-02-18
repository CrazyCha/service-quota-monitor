# 项目文件夹结构说明

本文档详细说明 AWS Service Quota Exporter 项目中各个文件夹的作用和用途。

---

## 📁 核心文件夹

### 1. `api/` - API 客户端封装层

**作用**：封装各个 AWS 服务的 API 调用，提供统一的接口

**子文件夹**：
- `api/aws/` - AWS 服务 API 客户端
  - `ec2.py` - EC2/EBS API 客户端（DescribeVolumes, DescribeSnapshots 等）
  - `elb.py` - ELB API 客户端（DescribeLoadBalancers 等）
  - `eks.py` - EKS API 客户端（ListClusters 等）
  - `elasticache.py` - ElastiCache API 客户端（DescribeCacheClusters 等）
  - `route53.py` - Route53 API 客户端（GetAccountLimit, GetHostedZoneCount 等）
  - `cloudfront.py` - CloudFront API 客户端（ListDistributions 等）
  - `sagemaker.py` - SageMaker API 客户端（ListNotebookInstances, ListTrainingJobs 等）
  - `calculator.py` - 使用量计算工具（汇总 API 返回的数据）

**职责**：
- 封装 boto3 客户端调用
- 处理 API 错误和重试
- 返回标准化的资源数据
- 不涉及业务逻辑，只负责数据获取

---

### 2. `provider/` - Provider 发现层

**作用**：提供账号、区域、凭证的发现和管理功能

**子文件夹**：

#### `provider/discovery/` - 账号和区域发现
- `interfaces.py` - 定义 Provider 接口（AccountProvider, RegionProvider, CredentialProvider）
- `cmdb_provider.py` - CMDB 模式实现（从 MySQL 数据库读取账号、区域、凭证）
- `active_region_discoverer.py` - EC2 Region 自动发现器（探测哪些 Region 有 EC2 实例）
- `credential_provider.py` - 凭证提供者接口和实现（CMDBCredentialProvider）

**职责**：
- 账号发现：从 CMDB 或配置文件获取 AWS 账号列表
- 区域发现：从 CMDB 或配置文件获取区域列表，或自动发现活跃区域
- 凭证管理：从 CMDB 或环境变量获取账号凭证（Access Key / Secret Key）

#### `provider/aws/` - AWS 服务配额相关
- `service_quotas.py` - Service Quotas API 客户端（GetServiceQuota, ListServiceQuotas）
- `usage_collector.py` - 使用量收集器接口和实现（8 个服务的 Usage 收集）
- `sagemaker_discovery.py` - SageMaker 配额动态发现（通过 ListServiceQuotas 发现配额）
- `cloudwatch.py` - CloudWatch API 客户端（用于部分配额的 Usage 采集）
- `provider.py` - AWS Provider 主类（协调 Service Quotas 和 Usage 采集）

**职责**：
- 调用 AWS Service Quotas API 获取配额 Limit
- 调用各服务 API 获取配额 Usage
- 处理 SageMaker 等服务的动态配额发现

---

### 3. `collector/` - Prometheus 指标收集器

**作用**：管理 Prometheus 指标，将采集到的配额数据转换为 Prometheus 格式

**文件**：
- `collector.py` - QuotaCollector 主类
  - 管理 3 个 Prometheus 指标：
    - `cloud_service_quota_limit` - 配额限制值
    - `cloud_service_quota_usage` - 配额使用量
    - `cloud_quota_usage_percent` - 配额使用百分比
  - 提供 `/metrics` 端点的数据
- `quota_result.py` - QuotaResult 数据类（封装采集结果）

**职责**：
- 接收采集到的配额数据（Limit 和 Usage）
- 更新 Prometheus 指标
- 提供指标数据供 HTTP 端点使用

---

### 4. `config/` - 配置文件管理

**作用**：加载和验证配额配置文件

**文件**：
- `quotas.yaml` - 配额配置文件（定义要采集的 8 个服务的配额列表）
- `loader.py` - 配置文件加载器（解析 YAML，转换为 QuotaConfig 对象）
- `validator.py` - 配置验证器（验证配置格式是否正确）

**职责**：
- 定义要采集的配额（quota_code, quota_name）
- 定义服务的区域类型（全局服务 vs 区域型服务）
- 定义 SageMaker Discovery 规则

---

### 5. `cache/` - 缓存层

**作用**：提供各种缓存功能，减少 API 调用，提升性能

**文件**：
- `quota_limit_cache.py` - 配额 Limit 文件缓存（24 小时 TTL）
  - 缓存路径：`.quota_limit_cache/{account_id}/{region}/{service}.json`
  - 大幅减少 GetServiceQuota API 调用
- `cache.py` - 通用缓存基类（内存缓存，用于 Usage 数据）

**职责**：
- Limit 缓存：文件缓存，24 小时有效期
- Usage 缓存：内存缓存，1 小时有效期
- 账号/区域缓存：文件缓存，24 小时有效期（在 provider 中实现）

---

### 6. `scheduler/` - 定时任务调度器

**作用**：定时执行配额采集任务

**文件**：
- `scheduler.py` - QuotaScheduler 定时任务调度器
  - Limit 采集：每 24 小时执行一次
  - Usage 采集：每 1 小时执行一次
  - 后台线程运行，不阻塞主进程

**职责**：
- 定时调用采集函数
- 不关心业务细节，只负责调度

---

### 7. `cloudwatch/` - CloudWatch 集成

**作用**：处理 CloudWatch 相关的配置和 API 调用

**文件**：
- `client.py` - CloudWatch API 客户端
- `loader.py` - CloudWatch Dimensions 配置加载器
- `cloudwatch-dimensions.yaml` - CloudWatch Dimensions 配置（1180 行）
- `cloudwatch-dimensions-additions.yaml` - CloudWatch Dimensions 补充配置（74 行）

**职责**：
- 部分配额的 Usage 需要通过 CloudWatch 指标获取
- 管理 CloudWatch Dimensions 配置

---

### 8. `quota/` - 配额匹配和加载

**作用**：配额匹配和加载逻辑

**文件**：
- `loader.py` - 配额加载器
- `matcher.py` - 配额匹配器（用于 SageMaker Discovery 的配额匹配）

**职责**：
- 处理 SageMaker 等服务的动态配额匹配
- 根据匹配规则过滤配额

---

### 9. `retry/` - 重试机制

**作用**：提供 API 调用的重试逻辑

**文件**：
- `retry.py` - 重试装饰器和工具函数
  - 处理 `TooManyRequestsException` 的指数退避重试
  - 处理其他临时性错误的重试

**职责**：
- 实现指数退避重试
- 处理 API 限流和临时错误

---

## 📄 根目录文件

### 核心文件

- `main.py` - **主程序入口**
  - 初始化所有组件
  - 执行初始采集
  - 启动定时任务
  - 启动 Flask HTTP 服务器（/metrics, /health 端点）

- `requirements.txt` - Python 依赖包列表
  - Flask - HTTP 服务器
  - prometheus-client - Prometheus 指标
  - PyYAML - YAML 配置文件解析
  - boto3 - AWS SDK
  - pymysql - MySQL 数据库连接（CMDB）

### 脚本文件

- `restart_exporter.sh` - 重启 Exporter 脚本
- `stop_exporter.sh` - 停止 Exporter 脚本
- `START_EXPORTER_CMDB.sh` - CMDB 模式启动脚本
- `package.sh` - 打包脚本（生成部署包）

### 工具文件

- `view_metrics.py` - 查看采集到的指标（调试用）
- `view_regions_simple.py` - 查看区域信息（调试用）
- `force_refresh_all_regions.py` - 强制刷新所有区域缓存（调试用）

---

## 🔄 数据流向

```
1. main.py 启动
   ↓
2. provider/discovery/ 发现账号和区域（从 CMDB 或配置文件）
   ↓
3. provider/aws/ 调用 AWS API 获取配额 Limit 和 Usage
   ↓
4. cache/ 缓存数据（减少后续 API 调用）
   ↓
5. collector/ 更新 Prometheus 指标
   ↓
6. Flask HTTP 服务器暴露 /metrics 端点
   ↓
7. scheduler/ 定时刷新数据
```

---

## 📊 模块依赖关系

```
main.py
├── config/          (加载配额配置)
├── provider/         (发现账号、区域、凭证)
│   ├── discovery/   (账号/区域发现)
│   └── aws/         (AWS API 调用)
├── api/aws/         (各服务 API 封装)
├── cache/            (缓存层)
├── collector/        (Prometheus 指标)
├── scheduler/        (定时任务)
├── retry/            (重试机制)
└── cloudwatch/       (CloudWatch 集成)
```

---

## 🎯 总结

- **api/** - 封装 AWS API 调用
- **provider/** - 账号/区域/凭证发现和管理
- **collector/** - Prometheus 指标管理
- **config/** - 配额配置加载
- **cache/** - 缓存层（性能优化）
- **scheduler/** - 定时任务调度
- **cloudwatch/** - CloudWatch 集成
- **quota/** - 配额匹配逻辑
- **retry/** - 重试机制

每个文件夹都有明确的职责，遵循单一职责原则，便于维护和扩展。

---

**最后更新**：2025-12-27

