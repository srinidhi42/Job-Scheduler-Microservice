"""
Job Scheduler module for the Job Scheduler Microservice.

Handles job scheduling, management, and coordination with execution engine.
"""

import uuid
import asyncio
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
from croniter import croniter
import heapq
import logging
from concurrent.futures import ThreadPoolExecutor
import json
import time

from .exceptions import (
    JobSchedulerError,
    JobNotFoundError,
    JobValidationError,
    JobConcurrencyError,
    ConfigurationError
)

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"


class JobPriority(Enum):
    """Job priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class ScheduleType(Enum):
    """Types of job schedules."""
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    CRON = "cron"
    INTERVAL = "interval"


@dataclass
class JobSchedule:
    """Job schedule configuration."""
    schedule_type: ScheduleType
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    max_executions: Optional[int] = None
    timezone: str = "UTC"
    
    def __post_init__(self):
        """Validate schedule configuration."""
        if self.schedule_type == ScheduleType.CRON and not self.cron_expression:
            raise JobValidationError("Cron expression required for CRON schedule type")
        
        if self.schedule_type == ScheduleType.INTERVAL and not self.interval_seconds:
            raise JobValidationError("Interval seconds required for INTERVAL schedule type")
        
        if self.cron_expression:
            try:
                croniter(self.cron_expression)
            except Exception as e:
                raise JobValidationError(f"Invalid cron expression: {e}")
    
    def get_next_run_time(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculate next run time based on schedule."""
        base_time = from_time or datetime.utcnow()
        
        if self.schedule_type == ScheduleType.IMMEDIATE:
            return base_time
        
        elif self.schedule_type == ScheduleType.DELAYED:
            return self.start_time or base_time
        
        elif self.schedule_type == ScheduleType.CRON:
            if not self.cron_expression:
                return None
            
            cron = croniter(self.cron_expression, base_time)
            next_time = cron.get_next(datetime)
            
            # Check if within time bounds
            if self.end_time and next_time > self.end_time:
                return None
            
            return next_time
        
        elif self.schedule_type == ScheduleType.INTERVAL:
            if not self.interval_seconds:
                return None
            
            next_time = base_time + timedelta(seconds=self.interval_seconds)
            
            # Check if within time bounds
            if self.end_time and next_time > self.end_time:
                return None
            
            return next_time
        
        return None


@dataclass
class JobMetadata:
    """Job metadata and configuration."""
    name: str
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    team: Optional[str] = None
    environment: str = "production"
    timeout_seconds: int = 3600  # 1 hour default
    max_retries: int = 3
    retry_delay_seconds: int = 60
    max_runtime_seconds: Optional[int] = None
    depends_on: List[str] = field(default_factory=list)  # Job dependencies
    resources: Dict[str, Any] = field(default_factory=dict)  # Resource requirements
    
    def validate(self) -> List[str]:
        """Validate job metadata."""
        errors = []
        
        if not self.name or not self.name.strip():
            errors.append("Job name cannot be empty")
        
        if self.timeout_seconds <= 0:
            errors.append("Timeout must be positive")
        
        if self.max_retries < 0:
            errors.append("Max retries cannot be negative")
        
        if self.retry_delay_seconds < 0:
            errors.append("Retry delay cannot be negative")
        
        return errors


@dataclass
class JobExecution:
    """Single job execution record."""
    execution_id: str
    job_id: str
    status: JobStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    retry_count: int = 0
    resource_usage: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Get execution duration."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data['status'] = self.status.value
        if self.started_at:
            data['started_at'] = self.started_at.isoformat()
        if self.completed_at:
            data['completed_at'] = self.completed_at.isoformat()
        return data


@dataclass
class ScheduledJob:
    """Complete job definition with schedule and metadata."""
    id: str
    metadata: JobMetadata
    schedule: JobSchedule
    command: str
    arguments: List[str] = field(default_factory=list)
    environment_variables: Dict[str, str] = field(default_factory=dict)
    working_directory: Optional[str] = None
    priority: JobPriority = JobPriority.NORMAL
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    next_run_time: Optional[datetime] = None
    last_run_time: Optional[datetime] = None
    execution_count: int = 0
    executions: List[JobExecution] = field(default_factory=list)
    
    def __post_init__(self):
        """Initialize job after creation."""
        if not self.next_run_time:
            self.next_run_time = self.schedule.get_next_run_time()
    
    def validate(self) -> List[str]:
        """Validate complete job definition."""
        errors = []
        
        # Validate metadata
        errors.extend(self.metadata.validate())
        
        # Validate command
        if not self.command or not self.command.strip():
            errors.append("Command cannot be empty")
        
        # Validate schedule constraints
        if (self.schedule.max_executions and 
            self.execution_count >= self.schedule.max_executions):
            errors.append("Job has reached maximum execution count")
        
        return errors
    
    def can_run(self) -> bool:
        """Check if job can be executed now."""
        if not self.enabled:
            return False
        
        if not self.next_run_time:
            return False
        
        if self.next_run_time > datetime.utcnow():
            return False
        
        if (self.schedule.max_executions and 
            self.execution_count >= self.schedule.max_executions):
            return False
        
        return True
    
    def update_next_run_time(self):
        """Update next run time based on schedule."""
        if self.schedule.schedule_type in [ScheduleType.IMMEDIATE, ScheduleType.DELAYED]:
            # One-time jobs
            self.next_run_time = None
        else:
            # Recurring jobs
            self.next_run_time = self.schedule.get_next_run_time()
    
    def add_execution(self, execution: JobExecution):
        """Add execution record."""
        self.executions.append(execution)
        self.execution_count += 1
        self.last_run_time = execution.started_at or datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def get_last_execution(self) -> Optional[JobExecution]:
        """Get most recent execution."""
        return self.executions[-1] if self.executions else None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data['priority'] = self.priority.value
        data['created_at'] = self.created_at.isoformat()
        data['updated_at'] = self.updated_at.isoformat()
        if self.next_run_time:
            data['next_run_time'] = self.next_run_time.isoformat()
        if self.last_run_time:
            data['last_run_time'] = self.last_run_time.isoformat()
        data['executions'] = [exec.to_dict() for exec in self.executions]
        return data


class JobQueue:
    """Priority queue for managing scheduled jobs."""
    
    def __init__(self):
        self._heap: List[tuple] = []
        self._index = 0
        self._lock = threading.Lock()
    
    def push(self, job: ScheduledJob):
        """Add job to queue."""
        with self._lock:
            if job.next_run_time:
                # Use negative priority for max-heap behavior (higher priority first)
                priority = (-job.priority.value, job.next_run_time.timestamp(), self._index)
                heapq.heappush(self._heap, (priority, job))
                self._index += 1
    
    def pop(self) -> Optional[ScheduledJob]:
        """Remove and return next job to run."""
        with self._lock:
            while self._heap:
                _, job = heapq.heappop(self._heap)
                if job.can_run():
                    return job
                # Re-queue if not ready yet
                if job.next_run_time and job.next_run_time > datetime.utcnow():
                    self.push(job)
            return None
    
    def peek(self) -> Optional[ScheduledJob]:
        """Look at next job without removing it."""
        with self._lock:
            if self._heap:
                return self._heap[0][1]
            return None
    
    def remove(self, job_id: str) -> bool:
        """Remove job from queue."""
        with self._lock:
            for i, (_, job) in enumerate(self._heap):
                if job.id == job_id:
                    del self._heap[i]
                    heapq.heapify(self._heap)
                    return True
            return False
    
    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._heap)
    
    def clear(self):
        """Clear all jobs from queue."""
        with self._lock:
            self._heap.clear()


class JobScheduler:
    """Main job scheduler coordinating job execution."""
    
    def __init__(
        self,
        max_concurrent_jobs: int = 10,
        cleanup_interval_seconds: int = 3600,
        job_timeout_seconds: int = 3600,
        enable_persistence: bool = True
    ):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.cleanup_interval_seconds = cleanup_interval_seconds
        self.job_timeout_seconds = job_timeout_seconds
        self.enable_persistence = enable_persistence
        
        # Job storage
        self.jobs: Dict[str, ScheduledJob] = {}
        self.job_queue = JobQueue()
        self.running_jobs: Dict[str, JobExecution] = {}
        
        # Concurrency control
        self.job_type_limits: Dict[str, int] = {}
        self.job_type_counts: Dict[str, int] = {}
        
        # Scheduling thread control
        self._scheduler_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()
        
        # Callbacks
        self.job_started_callbacks: List[Callable[[ScheduledJob, JobExecution], None]] = []
        self.job_completed_callbacks: List[Callable[[ScheduledJob, JobExecution], None]] = []
        self.job_failed_callbacks: List[Callable[[ScheduledJob, JobExecution], None]] = []
        
        logger.info(f"JobScheduler initialized with max_concurrent_jobs={max_concurrent_jobs}")
    
    def create_job(
        self,
        name: str,
        command: str,
        schedule: JobSchedule,
        arguments: Optional[List[str]] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        metadata: Optional[JobMetadata] = None,
        **kwargs
    ) -> ScheduledJob:
        """Create a new scheduled job."""
        job_id = str(uuid.uuid4())
        
        # Create metadata if not provided
        if metadata is None:
            metadata = JobMetadata(name=name, **kwargs)
        
        # Create job
        job = ScheduledJob(
            id=job_id,
            metadata=metadata,
            schedule=schedule,
            command=command,
            arguments=arguments or [],
            environment_variables=environment_variables or {},
            **kwargs
        )
        
        # Validate job
        validation_errors = job.validate()
        if validation_errors:
            raise JobValidationError(
                "Job validation failed",
                validation_errors=validation_errors
            )
        
        # Check concurrency limits
        self._check_concurrency_limits(job)
        
        # Store job
        with self._lock:
            self.jobs[job_id] = job
            if job.next_run_time:
                self.job_queue.push(job)
        
        logger.info(f"Created job: {name} (ID: {job_id})")
        return job
    
    def get_job(self, job_id: str) -> ScheduledJob:
        """Get job by ID."""
        job = self.jobs.get(job_id)
        if not job:
            raise JobNotFoundError(job_id)
        return job
    
    def update_job(self, job_id: str, **updates) -> ScheduledJob:
        """Update job configuration."""
        job = self.get_job(job_id)
        
        # Update fields
        for key, value in updates.items():
            if hasattr(job, key):
                setattr(job, key, value)
            elif hasattr(job.metadata, key):
                setattr(job.metadata, key, value)
        
        job.updated_at = datetime.utcnow()
        
        # Re-validate
        validation_errors = job.validate()
        if validation_errors:
            raise JobValidationError(
                "Job validation failed after update",
                validation_errors=validation_errors
            )
        
        # Update queue if schedule changed
        if 'schedule' in updates or 'enabled' in updates:
            self.job_queue.remove(job_id)
            if job.enabled and job.next_run_time:
                self.job_queue.push(job)
        
        logger.info(f"Updated job: {job.metadata.name} (ID: {job_id})")
        return job
    
    def delete_job(self, job_id: str) -> bool:
        """Delete job."""
        with self._lock:
            job = self.jobs.pop(job_id, None)
            if job:
                self.job_queue.remove(job_id)
                # Cancel if running
                if job_id in self.running_jobs:
                    self.cancel_job(job_id)
                logger.info(f"Deleted job: {job.metadata.name} (ID: {job_id})")
                return True
            return False
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel running job."""
        execution = self.running_jobs.get(job_id)
        if execution:
            execution.status = JobStatus.CANCELLED
            execution.completed_at = datetime.utcnow()
            logger.info(f"Cancelled job execution: {job_id}")
            return True
        return False
    
    def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        enabled: Optional[bool] = None,
        limit: Optional[int] = None
    ) -> List[ScheduledJob]:
        """List jobs with optional filtering."""
        jobs = list(self.jobs.values())
        
        # Apply filters
        if status is not None:
            jobs = [job for job in jobs 
                   if job.get_last_execution() and job.get_last_execution().status == status]
        
        if enabled is not None:
            jobs = [job for job in jobs if job.enabled == enabled]
        
        # Sort by next run time
        jobs.sort(key=lambda j: j.next_run_time or datetime.max)
        
        # Apply limit
        if limit:
            jobs = jobs[:limit]
        
        return jobs
    
    def get_job_executions(self, job_id: str, limit: Optional[int] = None) -> List[JobExecution]:
        """Get execution history for a job."""
        job = self.get_job(job_id)
        executions = job.executions.copy()
        executions.sort(key=lambda e: e.started_at or datetime.min, reverse=True)
        
        if limit:
            executions = executions[:limit]
        
        return executions
    
    def start(self):
        """Start the scheduler."""
        if self._running:
            return
        
        self._running = True
        
        # Start scheduler thread
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()
        
        logger.info("JobScheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        self._running = False
        
        # Wait for threads to finish
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        
        logger.info("JobScheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        while self._running:
            try:
                # Get next job to run
                job = self.job_queue.pop()
                if job:
                    self._execute_job(job)
                else:
                    # No jobs ready, sleep briefly
                    time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                time.sleep(1)
    
    def _cleanup_loop(self):
        """Cleanup loop for finished jobs and maintenance."""
        while self._running:
            try:
                self._cleanup_finished_jobs()
                self._update_job_schedules()
                time.sleep(self.cleanup_interval_seconds)
            
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)
                time.sleep(60)
    
    def _execute_job(self, job: ScheduledJob):
        """Execute a job (placeholder - actual execution handled by JobExecutor)."""
        if len(self.running_jobs) >= self.max_concurrent_jobs:
            # Re-queue for later
            self.job_queue.push(job)
            return
        
        # Create execution record
        execution = JobExecution(
            execution_id=str(uuid.uuid4()),
            job_id=job.id,
            status=JobStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        # Add to running jobs
        self.running_jobs[job.id] = execution
        job.add_execution(execution)
        
        # Update next run time
        job.update_next_run_time()
        if job.next_run_time:
            self.job_queue.push(job)
        
        # Trigger callbacks
        for callback in self.job_started_callbacks:
            try:
                callback(job, execution)
            except Exception as e:
                logger.error(f"Error in job started callback: {e}")
        
        logger.info(f"Started job execution: {job.metadata.name} (ID: {job.id})")
    
    def _cleanup_finished_jobs(self):
        """Clean up finished job executions."""
        finished_jobs = []
        
        for job_id, execution in self.running_jobs.items():
            if execution.status in [JobStatus.COMPLETED, JobStatus.FAILED, 
                                  JobStatus.CANCELLED, JobStatus.TIMEOUT]:
                finished_jobs.append(job_id)
        
        for job_id in finished_jobs:
            execution = self.running_jobs.pop(job_id)
            job = self.jobs.get(job_id)
            
            if job:
                # Trigger appropriate callbacks
                if execution.status == JobStatus.COMPLETED:
                    for callback in self.job_completed_callbacks:
                        try:
                            callback(job, execution)
                        except Exception as e:
                            logger.error(f"Error in job completed callback: {e}")
                else:
                    for callback in self.job_failed_callbacks:
                        try:
                            callback(job, execution)
                        except Exception as e:
                            logger.error(f"Error in job failed callback: {e}")
    
    def _update_job_schedules(self):
        """Update job schedules and re-queue jobs."""
        current_time = datetime.utcnow()
        
        for job in self.jobs.values():
            if (job.enabled and job.next_run_time and 
                job.next_run_time <= current_time and 
                job.id not in self.running_jobs):
                
                # Re-queue job if it's ready
                self.job_queue.push(job)
    
    def _check_concurrency_limits(self, job: ScheduledJob):
        """Check if job can run within concurrency limits."""
        job_type = job.metadata.tags[0] if job.metadata.tags else "default"
        
        if job_type in self.job_type_limits:
            current_count = self.job_type_counts.get(job_type, 0)
            if current_count >= self.job_type_limits[job_type]:
                raise JobConcurrencyError(
                    job_type=job_type,
                    current_jobs=current_count,
                    max_concurrent=self.job_type_limits[job_type]
                )
    
    def set_concurrency_limit(self, job_type: str, limit: int):
        """Set concurrency limit for job type."""
        self.job_type_limits[job_type] = limit
        logger.info(f"Set concurrency limit for {job_type}: {limit}")
    
    def add_job_started_callback(self, callback: Callable[[ScheduledJob, JobExecution], None]):
        """Add callback for job started events."""
        self.job_started_callbacks.append(callback)
    
    def add_job_completed_callback(self, callback: Callable[[ScheduledJob, JobExecution], None]):
        """Add callback for job completed events."""
        self.job_completed_callbacks.append(callback)
    
    def add_job_failed_callback(self, callback: Callable[[ScheduledJob, JobExecution], None]):
        """Add callback for job failed events."""
        self.job_failed_callbacks.append(callback)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        total_jobs = len(self.jobs)
        enabled_jobs = len([job for job in self.jobs.values() if job.enabled])
        running_jobs = len(self.running_jobs)
        queued_jobs = self.job_queue.size()
        
        return {
            "total_jobs": total_jobs,
            "enabled_jobs": enabled_jobs,
            "running_jobs": running_jobs,
            "queued_jobs": queued_jobs,
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "scheduler_running": self._running
        }