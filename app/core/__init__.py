"""
Core module for Job Scheduler Microservice.

This module provides the essential components for job scheduling, execution,
and security management.
"""

from .scheduler import JobScheduler, ScheduledJob
from .executor import JobExecutor, ExecutionResult, ExecutionStatus
from .exceptions import (
    JobSchedulerError,
    JobNotFoundError,
    JobExecutionError,
    JobValidationError,
    SecurityError,
    RateLimitExceededError,
    ResourceExhaustedError
)
from .security import SecurityManager, AuthenticationError, AuthorizationError

__version__ = "1.0.0"
__author__ = "Job Scheduler Team"

# Export all core components
__all__ = [
    # Scheduler components
    "JobScheduler",
    "ScheduledJob",
    
    # Executor components
    "JobExecutor", 
    "ExecutionResult",
    "ExecutionStatus",
    
    # Security components
    "SecurityManager",
    "AuthenticationError",
    "AuthorizationError",
    
    # Exception hierarchy
    "JobSchedulerError",
    "JobNotFoundError", 
    "JobExecutionError",
    "JobValidationError",
    "SecurityError",
    "RateLimitExceededError",
    "ResourceExhaustedError",
]

# Module metadata
CORE_COMPONENTS = {
    "scheduler": "Handles job scheduling and management",
    "executor": "Executes scheduled jobs with various strategies",
    "security": "Provides authentication, authorization and rate limiting",
    "exceptions": "Custom exception hierarchy for error handling"
}