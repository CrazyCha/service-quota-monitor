# -*- coding: utf-8 -*-
"""
CMDB Provider 实现

功能：
- 从 CMDB MySQL 数据库读取账号和凭证
- 只做 discovery，不涉及 AK/SK（凭证由 CredentialProvider 管理）
"""

import logging
import os
import json
import time
from typing import List, Dict, Optional, Tuple
from .interfaces import AccountProvider, RegionProvider

logger = logging.getLogger(__name__)

try:
    import pymysql
    PYMySQL_AVAILABLE = True
except ImportError:
    PYMySQL_AVAILABLE = False
    logger.warning("pymysql 未安装，CMDB Provider 将无法使用。请运行: pip install pymysql")


class CMDBAccountProvider(AccountProvider):
    """
    CMDB 账号 Provider（从 MySQL 数据库读取）
    
    从 audit_account 表读取 AWS 账号列表
    
    缓存机制：
    - 账号列表缓存 24 小时（86400 秒）
    - 缓存文件：.cmdb_accounts_cache/accounts.json
    - 可通过环境变量 ACCOUNTS_CACHE_TTL 调整缓存时间
    - 可通过环境变量 FORCE_REFRESH_ACCOUNTS=true 强制刷新
    """
    
    def __init__(self, 
                 db_host: str = None,
                 db_port: int = None,
                 db_name: str = None,
                 db_user: str = None,
                 db_password: str = None):
        """
        初始化 CMDB Account Provider
        
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
            raise ImportError("pymysql 未安装，无法使用 CMDB Provider。请运行: pip install pymysql")
        
        # 缓存配置（默认 24 小时）
        self.cache_dir = os.getenv('CMDB_ACCOUNTS_CACHE_DIR', '.cmdb_accounts_cache')
        self.cache_ttl = int(os.getenv('ACCOUNTS_CACHE_TTL', '86400'))  # 默认 24 小时（86400秒）
        
        # 创建缓存目录
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.info(f"初始化 CMDB Account Provider (host: {self.db_host}, db: {self.db_name})")
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
    
    def _load_accounts_cache(self) -> Optional[List[str]]:
        """从缓存文件加载账号列表（缓存 24h）"""
        cache_file = os.path.join(self.cache_dir, 'accounts.json')
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查缓存是否过期（24h）
            cache_time = cache_data.get('timestamp', 0)
            if time.time() - cache_time > self.cache_ttl:
                logger.debug(f"账号列表缓存已过期，需要重新查询")
                return None
            
            accounts = cache_data.get('accounts', [])
            logger.debug(f"从缓存加载账号列表: {len(accounts)} 个账号")
            return accounts
            
        except Exception as e:
            logger.warning(f"加载账号列表缓存失败: {e}")
            return None
    
    def _save_accounts_cache(self, accounts: List[str]):
        """保存账号列表到缓存文件（缓存 24h）"""
        cache_file = os.path.join(self.cache_dir, 'accounts.json')
        
        try:
            cache_data = {
                'timestamp': time.time(),
                'accounts': accounts
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"账号列表已保存到缓存: {cache_file}")
            
        except Exception as e:
            logger.warning(f"保存账号列表缓存失败: {e}")
    
    def _query_accounts_from_db(self) -> List[str]:
        """从数据库查询账号列表（内部方法）"""
        if not PYMySQL_AVAILABLE:
            logger.error("pymysql 未安装，无法查询数据库")
            return []
        
        connection = None
        try:
            connection = self._get_db_connection()
            
            with connection.cursor() as cursor:
                sql = """
                    SELECT accountId, access_key, secret_key
                    FROM cmdb_back_on.audit_account
                    WHERE origin = 'aws' AND is_freeze = '0'
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                
                accounts = []
                for row in results:
                    account_id = row.get('accountId')
                    if account_id:
                        accounts.append(str(account_id))
                
                logger.info(f"从 CMDB 数据库读取到 {len(accounts)} 个 AWS 账号")
                logger.debug(f"账号列表: {accounts}")
                return accounts
                
        except Exception as e:
            logger.error(f"从 CMDB 数据库读取账号列表失败: {e}", exc_info=True)
            return []
        finally:
            if connection:
                connection.close()
    
    def get_accounts(self, use_cache: bool = True, force_refresh: bool = False) -> List[str]:
        """
        获取账号列表（带缓存机制，默认缓存 24 小时）
        
        从 audit_account 表查询 AWS 账号，结果缓存 24 小时
        
        Args:
            use_cache: 是否使用缓存（默认 True）
            force_refresh: 是否强制刷新（忽略缓存，默认 False）
        
        Returns:
            账号 ID 列表
        """
        # 检查是否强制刷新
        if os.getenv('FORCE_REFRESH_ACCOUNTS', 'false').lower() == 'true':
            force_refresh = True
        
        # 检查缓存
        if use_cache and not force_refresh:
            cached_accounts = self._load_accounts_cache()
            if cached_accounts is not None:
                logger.debug(f"使用缓存的账号列表: {len(cached_accounts)} 个账号")
                return cached_accounts
        
        # 缓存未命中或过期，从数据库查询
        logger.info("从 CMDB 数据库查询账号列表...")
        accounts = self._query_accounts_from_db()
        
        # 保存到缓存
        if use_cache and accounts:
            self._save_accounts_cache(accounts)
        
        return accounts
    
    def _load_credentials_cache(self) -> Optional[Dict[str, Dict[str, str]]]:
        """从缓存文件加载账号凭证（缓存 24h）"""
        cache_file = os.path.join(self.cache_dir, 'credentials.json')
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查缓存是否过期（24h）
            cache_time = cache_data.get('timestamp', 0)
            if time.time() - cache_time > self.cache_ttl:
                logger.debug(f"账号凭证缓存已过期，需要重新查询")
                return None
            
            credentials = cache_data.get('credentials', {})
            logger.debug(f"从缓存加载账号凭证: {len(credentials)} 个账号")
            return credentials
            
        except Exception as e:
            logger.warning(f"加载账号凭证缓存失败: {e}")
            return None
    
    def _save_credentials_cache(self, credentials: Dict[str, Dict[str, str]]):
        """保存账号凭证到缓存文件（缓存 24h）"""
        cache_file = os.path.join(self.cache_dir, 'credentials.json')
        
        try:
            cache_data = {
                'timestamp': time.time(),
                'credentials': credentials
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"账号凭证已保存到缓存: {cache_file}")
            
        except Exception as e:
            logger.warning(f"保存账号凭证缓存失败: {e}")
    
    def _query_credentials_from_db(self) -> Dict[str, Dict[str, str]]:
        """从数据库查询账号凭证（内部方法）"""
        if not PYMySQL_AVAILABLE:
            logger.error("pymysql 未安装，无法查询数据库")
            return {}
        
        connection = None
        try:
            connection = self._get_db_connection()
            
            with connection.cursor() as cursor:
                sql = """
                    SELECT accountId, access_key, secret_key
                    FROM cmdb_back_on.audit_account
                    WHERE origin = 'aws' AND is_freeze = '0'
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                
                credentials = {}
                for row in results:
                    account_id = str(row.get('accountId', ''))
                    access_key = row.get('access_key', '')
                    secret_key = row.get('secret_key', '')
                    
                    if account_id and access_key and secret_key:
                        credentials[account_id] = {
                            'access_key': access_key,
                            'secret_key': secret_key
                        }
                
                logger.info(f"从 CMDB 数据库读取到 {len(credentials)} 个账号的凭证")
                return credentials
                
        except Exception as e:
            logger.error(f"从 CMDB 数据库读取账号凭证失败: {e}", exc_info=True)
            return {}
        finally:
            if connection:
                connection.close()
    
    def get_account_credentials(self, use_cache: bool = True, force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
        """
        获取账号及其凭证（带缓存机制，默认缓存 24 小时）
        
        从 audit_account 表查询 AWS 账号凭证，结果缓存 24 小时
        
        Args:
            use_cache: 是否使用缓存（默认 True）
            force_refresh: 是否强制刷新（忽略缓存，默认 False）
        
        Returns:
            字典，key 为 account_id，value 为 {'access_key': ..., 'secret_key': ...}
        """
        # 检查是否强制刷新
        if os.getenv('FORCE_REFRESH_ACCOUNTS', 'false').lower() == 'true':
            force_refresh = True
        
        # 检查缓存
        if use_cache and not force_refresh:
            cached_credentials = self._load_credentials_cache()
            if cached_credentials is not None:
                logger.debug(f"使用缓存的账号凭证: {len(cached_credentials)} 个账号")
                return cached_credentials
        
        # 缓存未命中或过期，从数据库查询
        logger.info("从 CMDB 数据库查询账号凭证...")
        credentials = self._query_credentials_from_db()
        
        # 保存到缓存
        if use_cache and credentials:
            self._save_credentials_cache(credentials)
        
        return credentials
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "cmdb"


class CMDBRegionProvider(RegionProvider):
    """
    CMDB 区域 Provider（返回账号实际使用过 EC2 的 Region）
    
    策略：
    - Region 候选集从 CMDB 读取（视为静态输入，月级更新）
    - 返回账号实际使用过 EC2 的 Region（有实例的 Region）
    - 使用 ActiveRegionDiscoverer 进行 EC2 使用发现
    - 结果按 account_id 缓存 24h
    """
    
    def __init__(self, account_provider=None, region_discoverer=None, active_regions_map: Dict[str, List[str]] = None):
        """
        初始化 CMDB Region Provider
        
        Args:
            account_provider: CMDBAccountProvider 实例（用于获取账号凭证）
            region_discoverer: ActiveRegionDiscoverer 实例（用于 EC2 使用发现）
            active_regions_map: 预发现的活跃 Region 映射 {account_id: [region1, ...]}（可选，用于避免重复发现）
        """
        self.account_provider = account_provider
        self.region_discoverer = region_discoverer
        self._ec2_regions_cache: Dict[str, List[str]] = {}  # account_id -> regions
        
        # 如果提供了预发现的活跃 Region 映射，直接使用
        if active_regions_map:
            self._ec2_regions_cache.update(active_regions_map)
            logger.info(f"CMDB Region Provider 使用预发现的活跃 Region 映射（{len(active_regions_map)} 个账号）")
        
        logger.info("初始化 CMDB Region Provider（基于 EC2 使用发现）")
    
    def _load_ec2_regions(self, account_id: str) -> List[str]:
        """
        加载账号实际使用过 EC2 的 Region
        
        Args:
            account_id: 账号 ID
        
        Returns:
            Region 列表（只包含有 EC2 实例的 Region）
        """
        # 如果已缓存，直接返回
        if account_id in self._ec2_regions_cache:
            return self._ec2_regions_cache[account_id]
        
        # 如果没有 discoverer，返回空列表
        if not self.region_discoverer or not self.account_provider:
            logger.warning(f"Region Discoverer 未初始化，无法获取账号 {account_id} 的 EC2 Region")
            return []
        
        # 发现该账号的 EC2 Region（使用缓存）
        try:
            ec2_regions_map = self.region_discoverer.discover_ec2_used_regions_from_provider(
                self.account_provider,
                use_cache=True,
                force_refresh=False
            )
            
            regions = ec2_regions_map.get(account_id, [])
            self._ec2_regions_cache[account_id] = regions
            return regions
            
        except Exception as e:
            logger.error(f"获取账号 {account_id} 的 EC2 Region 失败: {e}")
            return []
    
    def get_regions(self, account_id: str = None) -> List[str]:
        """
        获取区域列表（返回账号实际使用过 EC2 的 Region）
        
        Args:
            account_id: 账号 ID（必需，用于获取该账号的 EC2 Region）
        
        Returns:
            Region 列表（只包含有 EC2 实例的 Region）
        """
        if not account_id:
            logger.warning("account_id 未提供，无法获取 EC2 Region")
            return []
        
        regions = self._load_ec2_regions(account_id)
        logger.debug(f"账号 {account_id} 的 EC2 Region: {len(regions)} 个 - {regions}")
        return regions
    
    def get_provider_type(self) -> str:
        """获取 Provider 类型"""
        return "cmdb"

