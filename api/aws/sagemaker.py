# -*- coding: utf-8 -*-
"""
SageMaker API 客户端模块

功能：
- 封装 SageMaker API 调用（ListNotebookInstances, ListTrainingJobs, ListEndpoints 等）
- 获取 SageMaker 资源使用信息
- 使用免费的 List API，控制成本
"""

import boto3
import logging
import time
from typing import List, Dict, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class SageMakerClient:
    """
    SageMaker API 客户端
    
    功能：
    - 调用 SageMaker API 获取资源信息
    - 返回标准化的资源数据
    - 使用免费的 List API，控制成本
    """
    
    def __init__(self, region: str = 'us-east-1', access_key: str = None, secret_key: str = None):
        """
        初始化 SageMaker 客户端
        
        Args:
            region: AWS 区域
            access_key: AWS Access Key（可选，如果提供则使用指定凭证）
            secret_key: AWS Secret Key（可选，如果提供则使用指定凭证）
        """
        self.region = region
        try:
            # 如果提供了 access_key 和 secret_key，使用指定凭证
            if access_key and secret_key:
                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
                self.client = session.client('sagemaker', region_name=region)
                logger.debug(f"SageMaker 客户端初始化成功（使用指定凭证），区域: {region}")
            else:
                # 使用默认凭证链（环境变量、配置文件、IAM 角色等）
                self.client = boto3.client('sagemaker', region_name=region)
                logger.debug(f"SageMaker 客户端初始化成功（使用默认凭证链），区域: {region}")
        except Exception as e:
            logger.error(f"初始化 SageMaker 客户端失败: {e}")
            raise
    
    def list_notebook_instances(self, status_filter: str = None) -> List[Dict]:
        """
        列出所有 Notebook Instances
        
        Args:
            status_filter: 状态过滤（可选，如 'InService', 'Stopped' 等）
        
        Returns:
            Notebook Instance 列表，每个包含 NotebookInstanceName, InstanceType, Status 等字段
        
        成本：免费（List API）
        """
        try:
            logger.debug(f"调用 ListNotebookInstances (region: {self.region}, status_filter: {status_filter})")
            
            notebook_instances = []
            paginator = self.client.get_paginator('list_notebook_instances')
            
            # 构建请求参数
            paginate_params = {}
            if status_filter:
                paginate_params['StatusEquals'] = status_filter
            
            # 分页获取所有 Notebook Instances
            for page in paginator.paginate(**paginate_params):
                for instance in page.get('NotebookInstances', []):
                    notebook_instances.append({
                        'NotebookInstanceName': instance.get('NotebookInstanceName', ''),
                        'InstanceType': instance.get('InstanceType', ''),
                        'Status': instance.get('NotebookInstanceStatus', ''),
                        'CreationTime': instance.get('CreationTime'),
                        'LastModifiedTime': instance.get('LastModifiedTime')
                    })
            
            logger.debug(f"列出 Notebook Instances 成功: 共 {len(notebook_instances)} 个")
            return notebook_instances
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"列出 Notebook Instances 失败: {error_code}: {e}")
            raise
        except Exception as e:
            logger.error(f"列出 Notebook Instances 失败: {e}")
            raise
    
    def list_training_jobs(self, status_filter: str = None) -> List[Dict]:
        """
        列出所有 Training Jobs
        
        Args:
            status_filter: 状态过滤（可选，如 'InProgress', 'Completed', 'Failed' 等）
        
        Returns:
            Training Job 列表，每个包含 TrainingJobName, TrainingJobStatus 等字段
        
        成本：免费（List API）
        """
        try:
            logger.debug(f"调用 ListTrainingJobs (region: {self.region}, status_filter: {status_filter})")
            
            training_jobs = []
            paginator = self.client.get_paginator('list_training_jobs')
            
            # 构建请求参数
            paginate_params = {}
            if status_filter:
                paginate_params['StatusEquals'] = status_filter
            
            # 分页获取所有 Training Jobs
            page_count = 0
            for page in paginator.paginate(**paginate_params):
                page_count += 1
                page_jobs = page.get('TrainingJobSummaries', [])
                for job in page_jobs:
                    training_jobs.append({
                        'TrainingJobName': job.get('TrainingJobName', ''),
                        'TrainingJobStatus': job.get('TrainingJobStatus', ''),
                        'CreationTime': job.get('CreationTime'),
                        'TrainingEndTime': job.get('TrainingEndTime')
                    })
                # 每 10 页输出一次进度（避免日志过多）
                if page_count % 10 == 0:
                    logger.debug(f"已获取 {len(training_jobs)} 个 Training Jobs（第 {page_count} 页）...")
            
            logger.debug(f"列出 Training Jobs 成功: 共 {len(training_jobs)} 个（{page_count} 页）")
            return training_jobs
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"列出 Training Jobs 失败: {error_code}: {e}")
            raise
        except Exception as e:
            logger.error(f"列出 Training Jobs 失败: {e}")
            raise
    
    def list_endpoints(self, status_filter: str = None) -> List[Dict]:
        """
        列出所有 Endpoints
        
        Args:
            status_filter: 状态过滤（可选，如 'InService', 'Creating', 'Failed' 等）
        
        Returns:
            Endpoint 列表，每个包含 EndpointName, EndpointStatus 等字段
        
        成本：免费（List API）
        """
        try:
            logger.debug(f"调用 ListEndpoints (region: {self.region}, status_filter: {status_filter})")
            
            endpoints = []
            paginator = self.client.get_paginator('list_endpoints')
            
            # 构建请求参数
            paginate_params = {}
            if status_filter:
                paginate_params['StatusEquals'] = status_filter
            
            # 分页获取所有 Endpoints
            page_count = 0
            for page in paginator.paginate(**paginate_params):
                page_count += 1
                page_endpoints = page.get('Endpoints', [])
                for endpoint in page_endpoints:
                    endpoints.append({
                        'EndpointName': endpoint.get('EndpointName', ''),
                        'EndpointStatus': endpoint.get('EndpointStatus', ''),
                        'CreationTime': endpoint.get('CreationTime'),
                        'LastModifiedTime': endpoint.get('LastModifiedTime')
                    })
                # 每 10 页输出一次进度（避免日志过多）
                if page_count % 10 == 0:
                    logger.debug(f"已获取 {len(endpoints)} 个 Endpoints（第 {page_count} 页）...")
            
            logger.debug(f"列出 Endpoints 成功: 共 {len(endpoints)} 个（{page_count} 页）")
            return endpoints
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"列出 Endpoints 失败: {error_code}: {e}")
            raise
        except Exception as e:
            logger.error(f"列出 Endpoints 失败: {e}")
            raise
    
    def get_notebook_instance_count(self, status_filter: str = None) -> int:
        """
        获取 Notebook Instance 数量（优化版：只计数，不获取详细信息）
        
        Args:
            status_filter: 状态过滤（可选）
        
        Returns:
            Notebook Instance 数量
        
        优化：直接计数，不构建完整对象列表，提高性能
        """
        try:
            logger.debug(f"获取 Notebook Instance 数量 (region: {self.region}, status_filter: {status_filter})")
            
            count = 0
            paginator = self.client.get_paginator('list_notebook_instances')
            
            # 构建请求参数
            paginate_params = {}
            if status_filter:
                paginate_params['StatusEquals'] = status_filter
            
            # 只计数，不构建对象列表
            for page in paginator.paginate(**paginate_params):
                count += len(page.get('NotebookInstances', []))
            
            logger.debug(f"Notebook Instance 数量: {count}")
            return count
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"获取 Notebook Instance 数量失败: {error_code}: {e}")
            raise
        except Exception as e:
            logger.error(f"获取 Notebook Instance 数量失败: {e}")
            raise
    
    def get_training_job_count(self, status_filter: str = None, max_pages: int = 100, timeout_seconds: int = 30) -> int:
        """
        获取 Training Job 数量（优化版：只计数，不获取详细信息）
        
        Args:
            status_filter: 状态过滤（可选，如 'InProgress', 'Completed', 'Failed' 等）
                         推荐使用 'InProgress' 只统计运行中的任务（配额通常针对并发运行的任务）
            max_pages: 最大页数限制（默认 100 页），避免无限等待
            timeout_seconds: 超时时间（秒，默认 30 秒），超过时间后返回当前计数
        
        Returns:
            Training Job 数量
        
        优化：
        - 直接计数，不构建完整对象列表，提高性能
        - 使用状态过滤，只统计运行中的任务
        - 添加超时和最大页数限制，避免无限等待
        - 如果页数太多但计数很少，提前退出
        """
        try:
            logger.debug(f"获取 Training Job 数量 (region: {self.region}, status_filter: {status_filter}, max_pages: {max_pages}, timeout: {timeout_seconds}秒)")
            
            count = 0
            paginator = self.client.get_paginator('list_training_jobs')
            
            # 构建请求参数
            paginate_params = {}
            if status_filter:
                paginate_params['StatusEquals'] = status_filter
                logger.debug(f"使用状态过滤: {status_filter}（只统计运行中的任务，加快速度）")
            
            # 只计数，不构建对象列表
            page_count = 0
            start_time = time.time()
            last_count = 0
            no_progress_pages = 0  # 连续无进展的页数
            
            for page in paginator.paginate(**paginate_params):
                page_count += 1
                page_jobs = page.get('TrainingJobSummaries', [])
                page_count_value = len(page_jobs)
                count += page_count_value
                
                elapsed = time.time() - start_time
                
                # 检查超时
                if elapsed > timeout_seconds:
                    logger.warning(f"Training Job 统计超时（{timeout_seconds}秒），已处理 {page_count} 页，当前计数: {count} 个")
                    logger.warning(f"返回当前计数作为估算值。如果需要完整统计，请增加超时时间")
                    break
                
                # 检查最大页数限制
                if page_count >= max_pages:
                    logger.info(f"达到最大页数限制（{max_pages}页），已处理 {page_count} 页，当前计数: {count} 个")
                    logger.info(f"返回当前计数。实际数量可能更多，但通常运行中的任务不会超过这个数量")
                    break
                
                # 检查是否有进展（如果连续很多页都没有新任务，可能是 API 分页问题）
                if page_count_value == 0:
                    no_progress_pages += 1
                    # 如果连续 50 页都没有新任务，提前退出
                    if no_progress_pages >= 50:
                        logger.warning(f"连续 {no_progress_pages} 页没有新任务，可能是 API 分页问题，提前退出")
                        logger.warning(f"已处理 {page_count} 页，当前计数: {count} 个 Training Jobs")
                        break
                else:
                    no_progress_pages = 0  # 重置计数器
                
                # 每 50 页输出一次进度
                if page_count % 50 == 0:
                    logger.debug(f"已处理 {page_count} 页，当前计数: {count} 个 Training Jobs（耗时: {elapsed:.1f}秒）...")
                
                # 如果已经处理了很多页但计数很少，可能是 API 分页问题，提前退出
                if page_count > 200 and count < 10:
                    logger.warning(f"已处理 {page_count} 页但只找到 {count} 个 Training Jobs，"
                                 f"可能是 API 分页问题，提前退出以避免无限等待")
                    break
            
            elapsed = time.time() - start_time
            logger.debug(f"Training Job 数量: {count}（共 {page_count} 页，耗时: {elapsed:.1f}秒）")
            return count
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"获取 Training Job 数量失败: {error_code}: {e}")
            raise
        except Exception as e:
            logger.error(f"获取 Training Job 数量失败: {e}")
            raise
    
    def get_endpoint_count(self, status_filter: str = None) -> int:
        """
        获取 Endpoint 数量（优化版：只计数，不获取详细信息）
        
        Args:
            status_filter: 状态过滤（可选）
        
        Returns:
            Endpoint 数量
        
        优化：直接计数，不构建完整对象列表，提高性能
        """
        try:
            logger.debug(f"获取 Endpoint 数量 (region: {self.region}, status_filter: {status_filter})")
            
            count = 0
            paginator = self.client.get_paginator('list_endpoints')
            
            # 构建请求参数
            paginate_params = {}
            if status_filter:
                paginate_params['StatusEquals'] = status_filter
            
            # 只计数，不构建对象列表
            page_count = 0
            for page in paginator.paginate(**paginate_params):
                page_count += 1
                page_endpoints = page.get('Endpoints', [])
                count += len(page_endpoints)
                # 每 50 页输出一次进度（避免日志过多）
                if page_count % 50 == 0:
                    logger.debug(f"已处理 {page_count} 页，当前计数: {count} 个 Endpoints...")
            
            logger.debug(f"Endpoint 数量: {count}（共 {page_count} 页）")
            return count
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"获取 Endpoint 数量失败: {error_code}: {e}")
            raise
        except Exception as e:
            logger.error(f"获取 Endpoint 数量失败: {e}")
            raise

