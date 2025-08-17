"""
Job Executor module for the Job Scheduler Microservice.

Handles actual job execution with various execution strategies and resource management.
"""

import os
import sys
import subprocess
import threading
import asyncio
import time
import signal
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
import logging
import json
import tempfile
import shutil
from pathlib import Path
import docker
import kubernetes
from kubernetes import client, config

from .exceptions import (
    JobExecutionError,
    JobTimeoutError,
    ResourceExhaustedError,
    ConfigurationError
)

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    """Execution status enumeration."""
    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    KILLED = "killed"


class ExecutionStrategy(Enum):
    """Job execution strategies."""
    LOCAL_PROCESS = "local_process"
    DOCKER_CONTAINER = "docker_container"
    KUBERNETES_JOB = "kubernetes_job"
    THREAD_POOL = "thread_pool"
    PROCESS_POOL = "process_pool"


@dataclass
class ResourceLimits:
    """Resource limits for job execution."""
    max_memory_mb: Optional[int] = None
    max_cpu_percent: Optional[float] = None
    max_disk_mb: Optional[int] = None
    max_network_mb: Optional[int] = None
    max_execution_time: Optional[int] = None
    max_open_files: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "max_memory_mb": self.max_memory_mb,
            "max_cpu_percent": self.max_cpu_percent,
            "max_disk_mb": self.max_disk_mb,
            "max_network_mb": self.max_network_mb,
            "max_execution_time": self.max_execution_time,
            "max_open_files": self.max_open_files
        }


@dataclass
class ResourceUsage:
    """Resource usage metrics during execution."""
    peak_memory_mb: float = 0.0
    peak_cpu_percent: float = 0.0
    total_cpu_time: float = 0.0
    disk_read_mb: float = 0.0
    disk_write_mb: float = 0.0
    network_sent_mb: float = 0.0
    network_recv_mb: float = 0.0
    open_files_count: int = 0
    execution_time_seconds: float = 0.0
    
    def update_from_process(self, process: psutil.Process):
        """Update metrics from psutil process."""
        try:
            memory_info = process.memory_info()
            self.peak_memory_mb = max(self.peak_memory_mb, memory_info.rss / 1024 / 1024)
            
            cpu_percent = process.cpu_percent()
            self.peak_cpu_percent = max(self.peak_cpu_percent, cpu_percent)
            
            cpu_times = process.cpu_times()
            self.total_cpu_time = cpu_times.user + cpu_times.system
            
            io_counters = process.io_counters()
            self.disk_read_mb = io_counters.read_bytes / 1024 / 1024
            self.disk_write_mb = io_counters.write_bytes / 1024 / 1024
            
            self.open_files_count = process.num_fds() if hasattr(process, 'num_fds') else 0
            
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "peak_memory_mb": round(self.peak_memory_mb, 2),
            "peak_cpu_percent": round(self.peak_cpu_percent, 2),
            "total_cpu_time": round(self.total_cpu_time, 2),
            "disk_read_mb": round(self.disk_read_mb, 2),
            "disk_write_mb": round(self.disk_write_mb, 2),
            "network_sent_mb": round(self.network_sent_mb, 2),
            "network_recv_mb": round(self.network_recv_mb, 2),
            "open_files_count": self.open_files_count,
            "execution_time_seconds": round(self.execution_time_seconds, 2)
        }


@dataclass
class ExecutionResult:
    """Result of job execution."""
    execution_id: str
    job_id: str
    status: ExecutionStatus
    exit_code: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_time_seconds: Optional[float] = None
    resource_usage: Optional[ResourceUsage] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Get execution duration."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    @property
    def success(self) -> bool:
        """Check if execution was successful."""
        return self.status == ExecutionStatus.COMPLETED and (self.exit_code == 0 or self.exit_code is None)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "execution_id": self.execution_id,
            "job_id": self.job_id,
            "status": self.status.value,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "execution_time_seconds": self.execution_time_seconds,
            "resource_usage": self.resource_usage.to_dict() if self.resource_usage else None,
            "metadata": self.metadata
        }


class BaseExecutor:
    """Base class for job executors."""
    
    def __init__(self, strategy: ExecutionStrategy, resource_limits: Optional[ResourceLimits] = None):
        self.strategy = strategy
        self.resource_limits = resource_limits or ResourceLimits()
        self.running_executions: Dict[str, Future] = {}
        self._shutdown = False
    
    def execute(
        self, 
        execution_id: str,
        job_id: str,
        command: str,
        arguments: List[str],
        environment: Dict[str, str],
        working_directory: Optional[str] = None,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """Execute job and return result."""
        raise NotImplementedError("Subclasses must implement execute method")
    
    def cancel(self, execution_id: str) -> bool:
        """Cancel running execution."""
        future = self.running_executions.get(execution_id)
        if future:
            return future.cancel()
        return False
    
    def shutdown(self, wait: bool = True):
        """Shutdown executor."""
        self._shutdown = True
        if wait:
            for future in self.running_executions.values():
                future.cancel()


class LocalProcessExecutor(BaseExecutor):
    """Execute jobs as local processes."""
    
    def __init__(self, resource_limits: Optional[ResourceLimits] = None):
        super().__init__(ExecutionStrategy.LOCAL_PROCESS, resource_limits)
        self.process_monitor_interval = 1.0  # seconds
    
    def execute(
        self,
        execution_id: str,
        job_id: str,
        command: str,
        arguments: List[str],
        environment: Dict[str, str],
        working_directory: Optional[str] = None,
        timeout_seconds: Optional[int] = None
    ) -> ExecutionResult:
        """Execute job as local process."""
        result = ExecutionResult(
            execution_id=execution_id,
            job_id=job_id,
            status=ExecutionStatus.INITIALIZING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Prepare command
            full_command = [command] + arguments
            
            # Prepare environment
            exec_env = os.environ.copy()
            exec_env.update(environment)
            
            # Set working directory
            work_dir = working_directory or os.getcwd()
            
            # Apply resource limits
            timeout = timeout_seconds or self.resource_limits.max_execution_time
            
            logger.info(f"Starting process execution: {execution_id}")
            result.status = ExecutionStatus.RUNNING
            
            # Start process
            process = subprocess.Popen(
                full_command,
                cwd=work_dir,
                env=exec_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                preexec_fn=self._setup_process_limits if os.name != 'nt' else None
            )
            
            # Monitor process
            resource_usage = ResourceUsage()
            psutil_process = psutil.Process(process.pid)
            
            try:
                stdout, stderr = self._monitor_process(
                    process, psutil_process, resource_usage, timeout
                )
                
                result.exit_code = process.returncode
                result.stdout = stdout
                result.stderr = stderr
                result.status = ExecutionStatus.COMPLETED if process.returncode == 0 else ExecutionStatus.FAILED
                
            except subprocess.TimeoutExpired:
                # Kill process and children
                self._kill_process_tree(process.pid)
                result.status = ExecutionStatus.TIMEOUT
                result.error_message = f"Process exceeded timeout of {timeout} seconds"
                
            except Exception as e:
                self._kill_process_tree(process.pid)
                result.status = ExecutionStatus.FAILED
                result.error_message = str(e)
            
            result.completed_at = datetime.utcnow()
            result.execution_time_seconds = resource_usage.execution_time_seconds
            result.resource_usage = resource_usage
            
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            logger.error(f"Process execution failed: {e}", exc_info=True)
        
        return result
    
    def _monitor_process(
        self, 
        process: subprocess.Popen, 
        psutil_process: psutil.Process,
        resource_usage: ResourceUsage,
        timeout: Optional[int]
    ) -> Tuple[str, str]:
        """Monitor process execution and resource usage."""
        start_time = time.time()
        stdout_data = []
        stderr_data = []
        
        while process.poll() is None:
            current_time = time.time()
            resource_usage.execution_time_seconds = current_time - start_time
            
            # Check timeout
            if timeout and resource_usage.execution_time_seconds > timeout:
                raise subprocess.TimeoutExpired(process.args, timeout)
            
            # Update resource usage
            try:
                resource_usage.update_from_process(psutil_process)
                self._check_resource_limits(resource_usage)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            
            # Read output (non-blocking)
            try:
                if process.stdout:
                    line = process.stdout.readline()
                    if line:
                        stdout_data.append(line)
                
                if process.stderr:
                    line = process.stderr.readline()
                    if line:
                        stderr_data.append(line)
            except:
                pass
            
            time.sleep(self.process_monitor_interval)
        
        # Get remaining output
        try:
            remaining_stdout, remaining_stderr = process.communicate(timeout=5)
            if remaining_stdout:
                stdout_data.append(remaining_stdout)
            if remaining_stderr:
                stderr_data.append(remaining_stderr)
        except subprocess.TimeoutExpired:
            pass
        
        return ''.join(stdout_data), ''.join(stderr_data)
    
    def _setup_process_limits(self):
        """Set process resource limits (Unix only)."""
        try:
            import resource
            
            # Set memory limit
            if self.resource_limits.max_memory_mb:
                memory_bytes = self.resource_limits.max_memory_mb * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
            
            # Set file descriptor limit
            if self.resource_limits.max_open_files:
                resource.setrlimit(
                    resource.RLIMIT_NOFILE, 
                    (self.resource_limits.max_open_files, self.resource_limits.max_open_files)
                )
            
            # Set CPU time limit
            if self.resource_limits.max_execution_time:
                resource.setrlimit(
                    resource.RLIMIT_CPU, 
                    (self.resource_limits.max_execution_time, self.resource_limits.max_execution_time)
                )
                
        except ImportError:
            logger.warning("Resource module not available, resource limits not set")
        except Exception as e:
            logger.warning(f"Failed to set resource limits: {e}")
    
    def _check_resource_limits(self, resource_usage: ResourceUsage):
        """Check if resource limits are exceeded."""
        if (self.resource_limits.max_memory_mb and 
            resource_usage.peak_memory_mb > self.resource_limits.max_memory_mb):
            raise ResourceExhaustedError(
                "memory", 
                resource_usage.peak_memory_mb, 
                self.resource_limits.max_memory_mb
            )
        
        if (self.resource_limits.max_cpu_percent and 
            resource_usage.peak_cpu_percent > self.resource_limits.max_cpu_percent):
            raise ResourceExhaustedError(
                "cpu", 
                resource_usage.peak_cpu_percent, 
                self.resource_limits.max_cpu_percent
            )
    
    def _kill_process_tree(self, pid: int):
        """Kill process and all its children."""
        try:
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            
            # Terminate children first
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            
            # Wait for children to terminate
            gone, alive = psutil.wait_procs(children, timeout=3)
            
            # Kill remaining children
            for child in alive:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            
            # Terminate parent
            try:
                parent.terminate()
                parent.wait(timeout=3)
            except psutil.TimeoutExpired:
                parent.kill()
            except psutil.NoSuchProcess:
                pass
                
        except psutil.NoSuchProcess:
            pass


class DockerExecutor(BaseExecutor):
    """Execute jobs in Docker containers."""
    
    def __init__(self, resource_limits: Optional[ResourceLimits] = None):
        super().__init__(ExecutionStrategy.DOCKER_CONTAINER, resource_limits)
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Docker client: {e}")
    
    def execute(
        self,
        execution_id: str,
        job_id: str,
        command: str,
        arguments: List[str],
        environment: Dict[str, str],
        working_directory: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        image: str = "python:3.9-slim",
        **kwargs
    ) -> ExecutionResult:
        """Execute job in Docker container."""
        result = ExecutionResult(
            execution_id=execution_id,
            job_id=job_id,
            status=ExecutionStatus.INITIALIZING,
            started_at=datetime.utcnow()
        )
        
        container = None
        try:
            # Prepare command
            full_command = [command] + arguments
            
            # Resource limits for Docker
            mem_limit = None
            if self.resource_limits.max_memory_mb:
                mem_limit = f"{self.resource_limits.max_memory_mb}m"
            
            cpu_quota = None
            if self.resource_limits.max_cpu_percent:
                cpu_quota = int(100000 * (self.resource_limits.max_cpu_percent / 100))
            
            # Create container
            container = self.docker_client.containers.create(
                image=image,
                command=full_command,
                environment=environment,
                working_dir=working_directory or "/app",
                mem_limit=mem_limit,
                cpu_quota=cpu_quota,
                cpu_period=100000,
                network_disabled=False,
                remove=True,
                detach=True,
                name=f"job-{execution_id}"
            )
            
            logger.info(f"Starting Docker container: {container.id[:12]}")
            result.status = ExecutionStatus.RUNNING
            
            # Start container
            container.start()
            
            # Wait for completion with timeout
            timeout = timeout_seconds or self.resource_limits.max_execution_time
            try:
                exit_code = container.wait(timeout=timeout)['StatusCode']
                
                # Get logs
                logs = container.logs(stdout=True, stderr=True, stream=False)
                output = logs.decode('utf-8', errors='replace')
                
                result.exit_code = exit_code
                result.stdout = output
                result.status = ExecutionStatus.COMPLETED if exit_code == 0 else ExecutionStatus.FAILED
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    result.status = ExecutionStatus.TIMEOUT
                    result.error_message = f"Container exceeded timeout of {timeout} seconds"
                else:
                    result.status = ExecutionStatus.FAILED
                    result.error_message = str(e)
                
                # Force stop container
                try:
                    container.kill()
                except:
                    pass
            
            result.completed_at = datetime.utcnow()
            
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            logger.error(f"Docker execution failed: {e}", exc_info=True)
        
        finally:
            # Cleanup container
            if container:
                try:
                    container.remove(force=True)
                except:
                    pass
        
        return result


class KubernetesExecutor(BaseExecutor):
    """Execute jobs in Kubernetes cluster."""
    
    def __init__(self, resource_limits: Optional[ResourceLimits] = None, namespace: str = "default"):
        super().__init__(ExecutionStrategy.KUBERNETES_JOB, resource_limits)
        self.namespace = namespace
        
        try:
            # Try to load in-cluster config first
            try:
                config.load_incluster_config()
            except config.ConfigException:
                # Fall back to kubeconfig
                config.load_kube_config()
            
            self.k8s_batch_v1 = client.BatchV1Api()
            self.k8s_core_v1 = client.CoreV1Api()
            
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Kubernetes client: {e}")
    
    def execute(
        self,
        execution_id: str,
        job_id: str,
        command: str,
        arguments: List[str],
        environment: Dict[str, str],
        working_directory: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        image: str = "python:3.9-slim",
        **kwargs
    ) -> ExecutionResult:
        """Execute job in Kubernetes cluster."""
        result = ExecutionResult(
            execution_id=execution_id,
            job_id=job_id,
            status=ExecutionStatus.INITIALIZING,
            started_at=datetime.utcnow()
        )
        
        job_name = f"job-{execution_id}"
        
        try:
            # Create Kubernetes Job manifest
            job_manifest = self._create_job_manifest(
                job_name=job_name,
                image=image,
                command=command,
                arguments=arguments,
                environment=environment,
                working_directory=working_directory,
                timeout_seconds=timeout_seconds
            )
            
            # Create job
            self.k8s_batch_v1.create_namespaced_job(
                namespace=self.namespace,
                body=job_manifest
            )
            
            logger.info(f"Created Kubernetes job: {job_name}")
            result.status = ExecutionStatus.RUNNING
            
            # Wait for job completion
            timeout = timeout_seconds or self.resource_limits.max_execution_time or 3600
            completed = self._wait_for_job_completion(job_name, timeout)
            
            if completed:
                # Get job status and logs
                job_status = self._get_job_status(job_name)
                logs = self._get_job_logs(job_name)
                
                result.stdout = logs
                result.exit_code = 0 if job_status.get('succeeded', 0) > 0 else 1
                result.status = ExecutionStatus.COMPLETED if result.exit_code == 0 else ExecutionStatus.FAILED
            else:
                result.status = ExecutionStatus.TIMEOUT
                result.error_message = f"Job exceeded timeout of {timeout} seconds"
            
            result.completed_at = datetime.utcnow()
            
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            logger.error(f"Kubernetes execution failed: {e}", exc_info=True)
        
        finally:
            # Cleanup job
            try:
                self.k8s_batch_v1.delete_namespaced_job(
                    name=job_name,
                    namespace=self.namespace,
                    propagation_policy="Background"
                )
            except:
                pass
        
        return result
    
    def _create_job_manifest(
        self,
        job_name: str,
        image: str,
        command: str,
        arguments: List[str],
        environment: Dict[str, str],
        working_directory: Optional[str],
        timeout_seconds: Optional[int]
    ) -> Dict[str, Any]:
        """Create Kubernetes Job manifest."""
        env_vars = [{"name": k, "value": v} for k, v in environment.items()]
        
        # Resource requests and limits
        resources = {}
        if self.resource_limits.max_memory_mb:
            resources["limits"] = resources.get("limits", {})
            resources["limits"]["memory"] = f"{self.resource_limits.max_memory_mb}Mi"
        
        if self.resource_limits.max_cpu_percent:
            resources["limits"] = resources.get("limits", {})
            resources["limits"]["cpu"] = f"{self.resource_limits.max_cpu_percent / 100}"
        
        container_spec = {
            "name": job_name,
            "image": image,
            "command": [command] + arguments,
            "env": env_vars,
            "resources": resources
        }
        
        if working_directory:
            container_spec["workingDir"] = working_directory
        
        job_spec = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {
                    "job-scheduler": "true",
                    "execution-id": self.execution_id
                }
            },
            "spec": {
                "backoffLimit": 0,
                "ttlSecondsAfterFinished": 300,
                "template": {
                    "spec": {
                        "containers": [container_spec],
                        "restartPolicy": "Never"
                    }
                }
            }
        }
        
        # Add active deadline if timeout specified
        if timeout_seconds:
            job_spec["spec"]["activeDeadlineSeconds"] = timeout_seconds
        
        return job_spec
    
    def _wait_for_job_completion(self, job_name: str, timeout: int) -> bool:
        """Wait for Kubernetes job to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                job = self.k8s_batch_v1.read_namespaced_job_status(
                    name=job_name,
                    namespace=self.namespace
                )
                
                if job.status.completion_time or job.status.failed:
                    return True
                
            except Exception as e:
                logger.error(f"Error checking job status: {e}")
            
            time.sleep(5)
        
        return False
    
    def _get_job_status(self, job_name: str) -> Dict[str, Any]:
        """Get Kubernetes job status."""
        try:
            job = self.k8s_batch_v1.read_namespaced_job_status(
                name=job_name,
                namespace=self.namespace
            )
            
            return {
                "succeeded": job.status.succeeded or 0,
                "failed": job.status.failed or 0,
                "active": job.status.active or 0
            }
        except Exception as e:
            logger.error(f"Error getting job status: {e}")
            return {}
    
    def _get_job_logs(self, job_name: str) -> str:
        """Get logs from Kubernetes job pods."""
        try:
            # Get pods for this job
            pods = self.k8s_core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"job-name={job_name}"
            )
            
            logs = []
            for pod in pods.items:
                try:
                    pod_logs = self.k8s_core_v1.read_namespaced_pod_log(
                        name=pod.metadata.name,
                        namespace=self.namespace
                    )
                    logs.append(f"Pod {pod.metadata.name}:\n{pod_logs}")
                except Exception as e:
                    logs.append(f"Failed to get logs for pod {pod.metadata.name}: {e}")
            
            return "\n".join(logs)
            
        except Exception as e:
            logger.error(f"Error getting job logs: {e}")
            return f"Failed to retrieve logs: {e}"


class JobExecutor:
    """Main job executor that coordinates different execution strategies."""
    
    def __init__(
        self,
        default_strategy: ExecutionStrategy = ExecutionStrategy.LOCAL_PROCESS,
        max_concurrent_executions: int = 10,
        resource_limits: Optional[ResourceLimits] = None
    ):
        self.default_strategy = default_strategy
        self.max_concurrent_executions = max_concurrent_executions
        self.default_resource_limits = resource_limits or ResourceLimits()
        
        # Initialize executors
        self.executors: Dict[ExecutionStrategy, BaseExecutor] = {
            ExecutionStrategy.LOCAL_PROCESS: LocalProcessExecutor(resource_limits),
            ExecutionStrategy.DOCKER_CONTAINER: DockerExecutor(resource_limits),
            ExecutionStrategy.KUBERNETES_JOB: KubernetesExecutor(resource_limits),
        }
        
        # Execution tracking
        self.active_executions: Dict[str, ExecutionResult] = {}
        self.execution_history: List[ExecutionResult] = []
        self.execution_callbacks: List[Callable[[ExecutionResult], None]] = []
        
        # Thread pool for async execution
        self.thread_pool = ThreadPoolExecutor(max_workers=max_concurrent_executions)
        
        logger.info(f"JobExecutor initialized with strategy: {default_strategy.value}")
    
    def submit_job(
        self,
        execution_id: str,
        job_id: str,
        command: str,
        arguments: List[str] = None,
        environment: Dict[str, str] = None,
        working_directory: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        strategy: Optional[ExecutionStrategy] = None,
        resource_limits: Optional[ResourceLimits] = None,
        **kwargs
    ) -> Future[ExecutionResult]:
        """Submit job for execution."""
        if len(self.active_executions) >= self.max_concurrent_executions:
            raise ResourceExhaustedError(
                "concurrent_executions",
                len(self.active_executions),
                self.max_concurrent_executions
            )
        
        exec_strategy = strategy or self.default_strategy
        executor = self.executors.get(exec_strategy)
        
        if not executor:
            raise ConfigurationError(f"Executor not available for strategy: {exec_strategy}")
        
        # Create initial result
        result = ExecutionResult(
            execution_id=execution_id,
            job_id=job_id,
            status=ExecutionStatus.PENDING
        )
        
        self.active_executions[execution_id] = result
        
        # Submit to thread pool
        future = self.thread_pool.submit(
            self._execute_job,
            executor,
            execution_id,
            job_id,
            command,
            arguments or [],
            environment or {},
            working_directory,
            timeout_seconds,
            resource_limits,
            **kwargs
        )
        
        # Add completion callback
        future.add_done_callback(lambda f: self._on_execution_complete(execution_id, f))
        
        logger.info(f"Submitted job for execution: {execution_id} using {exec_strategy.value}")
        return future
    
    def _execute_job(
        self,
        executor: BaseExecutor,
        execution_id: str,
        job_id: str,
        command: str,
        arguments: List[str],
        environment: Dict[str, str],
        working_directory: Optional[str],
        timeout_seconds: Optional[int],
        resource_limits: Optional[ResourceLimits],
        **kwargs
    ) -> ExecutionResult:
        """Execute job using specified executor."""
        try:
            # Update executor resource limits if provided
            if resource_limits:
                executor.resource_limits = resource_limits
            
            # Execute job
            result = executor.execute(
                execution_id=execution_id,
                job_id=job_id,
                command=command,
                arguments=arguments,
                environment=environment,
                working_directory=working_directory,
                timeout_seconds=timeout_seconds,
                **kwargs
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Job execution failed: {e}", exc_info=True)
            return ExecutionResult(
                execution_id=execution_id,
                job_id=job_id,
                status=ExecutionStatus.FAILED,
                error_message=str(e),
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow()
            )
    
    def _on_execution_complete(self, execution_id: str, future: Future):
        """Handle execution completion."""
        try:
            result = future.result()
            
            # Update active executions
            if execution_id in self.active_executions:
                self.active_executions[execution_id] = result
            
            # Add to history
            self.execution_history.append(result)
            
            # Cleanup active executions
            self.active_executions.pop(execution_id, None)
            
            # Trigger callbacks
            for callback in self.execution_callbacks:
                try:
                    callback(result)
                except Exception as e:
                    logger.error(f"Error in execution callback: {e}")
            
            logger.info(f"Execution completed: {execution_id} with status {result.status.value}")
            
        except Exception as e:
            logger.error(f"Error handling execution completion: {e}", exc_info=True)
    
    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel running execution."""
        if execution_id in self.active_executions:
            # Try to cancel with each executor
            for executor in self.executors.values():
                if executor.cancel(execution_id):
                    logger.info(f"Cancelled execution: {execution_id}")
                    return True
        return False
    
    def get_execution_status(self, execution_id: str) -> Optional[ExecutionResult]:
        """Get current execution status."""
        return self.active_executions.get(execution_id)
    
    def get_execution_history(self, limit: Optional[int] = None) -> List[ExecutionResult]:
        """Get execution history."""
        history = sorted(self.execution_history, key=lambda r: r.started_at or datetime.min, reverse=True)
        return history[:limit] if limit else history
    
    def add_execution_callback(self, callback: Callable[[ExecutionResult], None]):
        """Add callback for execution completion."""
        self.execution_callbacks.append(callback)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics."""
        return {
            "active_executions": len(self.active_executions),
            "max_concurrent_executions": self.max_concurrent_executions,
            "total_executions": len(self.execution_history),
            "default_strategy": self.default_strategy.value,
            "available_strategies": [s.value for s in self.executors.keys()]
        }
    
    def shutdown(self, wait: bool = True):
        """Shutdown executor."""
        logger.info("Shutting down JobExecutor")
        
        # Shutdown all executors
        for executor in self.executors.values():
            executor.shutdown(wait=False)
        
        # Shutdown thread pool
        self.thread_pool.shutdown(wait=wait)
        
        logger.info("JobExecutor shutdown complete")