# -*- coding: utf-8 -*-
"""
Active Region Discoverer 模块

功能：
- 从 CMDB 数据库读取 AWS Region 候选集
- 对每个账号在每个 Region 进行轻量探测
- 发现每个账号的活跃 Region
"""

import logging
import os
import json
import time
import boto3
from typing import Dict, List, Optional, Set
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)

try:
    import pymysql
    PYMySQL_AVAILABLE = True
except ImportError:
    PYMySQL_AVAILABLE = False
    logger.warning("pymysql 未安装，Active Region Discoverer 将无法使用。请运行: pip install pymysql")


class ActiveRegionDiscoverer:
    """
    Active Region Discoverer
    
    功能：
    - 从 CMDB 数据库读取 AWS Region 候选集
    - 对每个账号在每个 Region 进行轻量探测（EC2 DescribeInstances）
    - 发现每个账号的活跃 Region
    """
    
    def __init__(self,
                 db_host: str = None,
                 db_port: int = None,
                 db_name: str = None,
                 db_user: str = None,
                 db_password: str = None):
        """
        初始化 Active Region Discoverer
        
        Args:
            db_host: 数据库主机（默认从环境变量或默认值读取）
            db_port: 数据库端口（默认从环境变量或默认值读取）
            db_name: 数据库名（默认从环境变量或默认值读取）
            db_user: 数据库用户（默认从环境变量或默认值读取）
            db_password: 数据库密码（默认从环境变量 DB_PASSWORD 读取）
        """
        # 数据库配置（从参数或环境变量读取）
        self.db_host = db_host or os.getenv('CMDB_DB_HOST', 'cmdb.c2gapem8vrrz.ap-southeast-1.rds.amazonaws.com')
        self.db_port = db_port or int(os.getenv('CMDB_DB_PORT', '3306'))
        self.db_name = db_name or os.getenv('CMDB_DB_NAME', 'cmdb_back_on')
        self.db_user = db_user or os.getenv('CMDB_DB_USER', 'pro_itoe')
        self.db_password = db_password or os.getenv('DB_PASSWORD')
        
        if not self.db_password:
            raise ValueError("数据库密码未配置，请设置环境变量 DB_PASSWORD")
        
        if not PYMySQL_AVAILABLE:
            raise ImportError("pymysql 未安装，无法使用 Active Region Discoverer。请运行: pip install pymysql")
        
        # 缓存配置（按 account_id 缓存 24h）
        self.cache_dir = os.getenv('EC2_REGIONS_CACHE_DIR', '.ec2_regions_cache')
        self.cache_ttl = int(os.getenv('EC2_REGIONS_CACHE_TTL', '86400'))  # 默认 24 小时（86400秒）
        
        # 创建缓存目录
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.info(f"初始化 EC2 Region 使用发现器 (host: {self.db_host}, db: {self.db_name})")
        logger.info(f"缓存目录: {self.cache_dir}, 缓存时间: {self.cache_ttl} 秒 ({self.cache_ttl // 3600} 小时)")
    
    def _get_db_connection(self):
        """获取数据库连接"""
        try:
            connection = pymysql.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10,
                read_timeout=10
            )
            return connection
        except Exception as e:
            logger.error(f"连接 CMDB 数据库失败: {e}")
            raise
    
    def get_region_candidates(self) -> List[str]:
        """
        从 CMDB 数据库读取 AWS Region 候选集
        
        查询条件：
        - cloud = 'aws'
        - 排除 global region
        
        Returns:
            Region 列表，例如: ['us-east-1', 'us-west-2', 'eu-west-1', ...]
        """
        if not PYMySQL_AVAILABLE:
            logger.error("pymysql 未安装，无法查询数据库")
            return []
        
        connection = None
        try:
            connection = self._get_db_connection()
            
            with connection.cursor() as cursor:
                # 使用 region 字段（匹配实际数据库表结构）
                sql = """
                    SELECT DISTINCT region
                    FROM cmdb_back_on.cloud_region
                    WHERE cloud IN ('aws')
                      AND region != 'global'
                      AND region IS NOT NULL
                      AND region != ''
                    ORDER BY region
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                
                regions = [row['region'] for row in results if row.get('region')]
                logger.info(f"从 CMDB 数据库读取到 {len(regions)} 个 AWS Region 候选: {regions}")
                return regions
                
        except Exception as e:
            logger.error(f"读取 Region 候选集失败: {e}", exc_info=True)
            return []
        finally:
            if connection:
                connection.close()
    
    def probe_ec2_usage(self, 
                        region: str, 
                        access_key: str, 
                        secret_key: str) -> bool:
        """
        探测指定账号在指定 Region 是否使用过 EC2（是否有实例）
        
        使用 EC2 DescribeInstances 检查是否有至少 1 个实例
        
        Args:
            region: AWS Region
            access_key: AWS Access Key
            secret_key: AWS Secret Key
        
        Returns:
            True: Region 有 EC2 实例（使用过该 Region）
            False: Region 无 EC2 实例或无法访问
        """
        try:
            # 使用 boto3 Session 创建客户端
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            ec2_client = session.client('ec2', region_name=region)
            
            # 检查是否有实例（MaxResults=5 用于轻量探测，只需要知道是否有实例）
            # 注意：MaxResults 限制返回的实例数量，但如果有多个 Reservation，可能只返回第一个
            # 所以我们遍历所有 Reservation 来统计实例数量
            response = ec2_client.describe_instances(MaxResults=5)
            
            # 统计实例数量（遍历所有 Reservation）
            instance_count = 0
            for reservation in response.get('Reservations', []):
                instances = reservation.get('Instances', [])
                instance_count += len(instances)
                # 如果找到至少一个实例，就可以返回了（不需要继续统计）
                if instance_count > 0:
                    break
            
            if instance_count > 0:
                logger.info(f"Region {region} 有 {instance_count} 个 EC2 实例（账号: {access_key[:8]}...）")
                return True
            else:
                logger.debug(f"Region {region} 无 EC2 实例（账号: {access_key[:8]}...）")
                return False
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            
            # 这些错误表示 Region 不可访问，跳过
            skip_codes = ['AuthFailure', 'OptInRequired', 'InvalidEndpoint', 
                         'InvalidClientTokenId', 'SignatureDoesNotMatch']
            
            if error_code in skip_codes:
                logger.debug(f"Region {region} 不可访问（{error_code}）: {error_message}")
                return False
            else:
                # 其他错误（如网络错误、限流等），记录警告但不影响其他 Region
                logger.warning(f"Region {region} 探测失败（{error_code}）: {error_message}")
                return False
                
        except BotoCoreError as e:
            # boto3 核心错误（如网络错误）
            logger.warning(f"Region {region} 探测失败（BotoCoreError）: {e}")
            return False
            
        except Exception as e:
            # 其他未知错误
            logger.warning(f"Region {region} 探测失败（未知错误）: {e}")
            return False
    
    def discover_ec2_used_regions(self, 
                                  account_credentials: Dict[str, Dict[str, str]]) -> Dict[str, List[str]]:
        """
        发现每个账号实际使用过 EC2 的 Region（有实例的 Region）
        
        Args:
            account_credentials: 账号凭证字典，格式为 {account_id: {'access_key': 'xxx', 'secret_key': 'xxx'}}
        
        Returns:
            {account_id: [region1, region2, ...]} 的映射结果（只包含有 EC2 实例的 Region）
        """
        # 获取 Region 候选集（从 CMDB 读取，视为静态输入）
        region_candidates = self.get_region_candidates()
        
        if not region_candidates:
            logger.warning("Region 候选集为空，无法进行探测")
            return {}
        
        logger.info(f"开始发现 EC2 使用过的 Region（{len(account_credentials)} 个账号，{len(region_candidates)} 个候选 Region）")
        
        result: Dict[str, List[str]] = {}
        
        # 遍历每个账号
        for account_id, credentials in account_credentials.items():
            access_key = credentials.get('access_key')
            secret_key = credentials.get('secret_key')
            
            if not access_key or not secret_key:
                logger.warning(f"账号 {account_id} 凭证不完整，跳过")
                continue
            
            logger.info(f"探测账号 {account_id} 使用过的 EC2 Region...")
            used_regions = []
            
            # 遍历每个候选 Region（仅在 CMDB 提供的候选 Region 内）
            for region in region_candidates:
                try:
                    if self.probe_ec2_usage(region, access_key, secret_key):
                        used_regions.append(region)
                        logger.debug(f"账号 {account_id} 在 Region {region} 有 EC2 实例")
                except Exception as e:
                    # 单个 Region 失败不影响其他 Region
                    logger.warning(f"账号 {account_id} 在 Region {region} 探测异常: {e}")
                    continue
            
            result[account_id] = used_regions
            logger.info(f"账号 {account_id} 在 {len(used_regions)} 个 Region 使用过 EC2: {used_regions}")
        
        logger.info(f"EC2 Region 发现完成，共 {len(result)} 个账号有 EC2 实例")
        return result
    
    def _load_account_cache(self, account_id: str) -> Optional[List[str]]:
        """从缓存文件加载单个账号的 EC2 Region 使用结果（按 account_id 缓存 24h）"""
        cache_file = os.path.join(self.cache_dir, f"{account_id}.json")
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查缓存是否过期（24h）
            cache_time = cache_data.get('timestamp', 0)
            if time.time() - cache_time > self.cache_ttl:
                logger.debug(f"账号 {account_id} 的缓存已过期，需要重新探测")
                return None
            
            regions = cache_data.get('regions', [])
            logger.debug(f"从缓存加载账号 {account_id} 的 EC2 Region: {len(regions)} 个")
            return regions
            
        except Exception as e:
            logger.warning(f"加载账号 {account_id} 的缓存失败: {e}")
            return None
    
    def _save_account_cache(self, account_id: str, regions: List[str]):
        """保存单个账号的 EC2 Region 使用结果到缓存文件（按 account_id 缓存 24h）"""
        cache_file = os.path.join(self.cache_dir, f"{account_id}.json")
        
        try:
            cache_data = {
                'timestamp': time.time(),
                'account_id': account_id,
                'regions': regions
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"账号 {account_id} 的 EC2 Region 结果已保存到缓存: {cache_file}")
            
        except Exception as e:
            logger.warning(f"保存账号 {account_id} 的缓存失败: {e}")
    
    def discover_ec2_used_regions_from_provider(self, 
                                                account_provider,
                                                use_cache: bool = True,
                                                force_refresh: bool = False) -> Dict[str, List[str]]:
        """
        从 AccountProvider 获取账号并发现 EC2 使用过的 Region（便捷方法，按 account_id 缓存 24h）
        
        策略：
        - Region 候选集只从 CMDB 读取（视为静态输入）
        - 仅在 CMDB 提供的候选 Region 内探测
        - 使用 ec2:DescribeInstances 检查是否有实例
        - 结果按 account_id 缓存 24h
        
        Args:
            account_provider: CMDBAccountProvider 实例
            use_cache: 是否使用缓存（默认 True）
            force_refresh: 是否强制刷新（忽略缓存，默认 False）
        
        Returns:
            {account_id: [region1, region2, ...]} 的映射结果（只包含有 EC2 实例的 Region）
        """
        # 获取 Region 候选集（从 CMDB 读取，视为静态输入）
        region_candidates = self.get_region_candidates()
        
        if not region_candidates:
            logger.warning("Region 候选集为空，无法进行探测")
            return {}
        
        # 获取账号列表（包含凭证）
        account_credentials = account_provider.get_account_credentials()
        
        if not account_credentials:
            logger.warning("账号列表为空，无法进行探测")
            return {}
        
        result: Dict[str, List[str]] = {}
        
        # 遍历每个账号（按 account_id 缓存）
        for account_id, credentials in account_credentials.items():
            access_key = credentials.get('access_key')
            secret_key = credentials.get('secret_key')
            
            if not access_key or not secret_key:
                logger.warning(f"账号 {account_id} 凭证不完整，跳过")
                continue
            
            # 检查缓存（按 account_id）
            if use_cache and not force_refresh:
                cached_regions = self._load_account_cache(account_id)
                if cached_regions is not None:
                    result[account_id] = cached_regions
                    logger.debug(f"账号 {account_id} 使用缓存的 EC2 Region: {len(cached_regions)} 个")
                    continue
            
            # 缓存未命中或过期，执行探测
            logger.info(f"探测账号 {account_id} 使用过的 EC2 Region（在 {len(region_candidates)} 个候选 Region 内）...")
            used_regions = []
            
            # 遍历每个候选 Region（仅在 CMDB 提供的候选 Region 内）
            for region in region_candidates:
                try:
                    if self.probe_ec2_usage(region, access_key, secret_key):
                        used_regions.append(region)
                        logger.debug(f"账号 {account_id} 在 Region {region} 有 EC2 实例")
                except Exception as e:
                    # 单个 Region 失败不影响其他 Region
                    logger.warning(f"账号 {account_id} 在 Region {region} 探测异常: {e}")
                    continue
            
            result[account_id] = used_regions
            
            # 保存到缓存（按 account_id）
            if use_cache:
                self._save_account_cache(account_id, used_regions)
            
            logger.info(f"账号 {account_id} 在 {len(used_regions)} 个 Region 使用过 EC2: {used_regions}")
        
        logger.info(f"EC2 Region 发现完成，共 {len(result)} 个账号")
        return result

