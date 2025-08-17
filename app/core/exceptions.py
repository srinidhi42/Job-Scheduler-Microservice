"""
Custom exceptions for the Job Scheduler Microservice.

This module defines a comprehensive exception hierarchy for handling
various error scenarios in job scheduling and execution.
"""

from typing import Optional, Dict, Any
import traceback
from datetime import datetime


class JobSchedulerError(Exception):
    """Base exception for all job scheduler related errors."""
    
    def __init__(
        self, 
        message: str, 
        error_code: str = "GENERAL_ERROR",
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.timestamp = datetime.utcnow()
        self.traceback_info = traceback.format_exc()
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API responses."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }
    
    def __str__(self) -> str:
        return f"[{self.error_code}] {self.message}"


class JobValidationError(JobSchedulerError):
    """Raised when job data validation fails."""
    
    def __init__(
        self, 
        message: str, 
        field: Optional[str] = None,
        value: Any = None,
        validation_errors: Optional[Dict[str, str]] = None
    ):
        details = {
            "field": field,
            "invalid_value": str(value) if value is not None else None,
            "validation_errors": validation_errors or {}
        }
        super().__init__(
            message, 
            error_code="VALIDATION_ERROR", 
            details=details
        )
        self.field = field
        self.value = value
        self.validation_errors = validation_errors or {}


class JobNotFoundError(JobSchedulerError):
    """Raised when a requested job cannot be found."""
    
    def __init__(self, job_id: str, message: Optional[str] = None):
        message = message or f"Job with ID '{job_id}' not found"
        super().__init__(
            message, 
            error_code="JOB_NOT_FOUND",
            details={"job_id": job_id}
        )
        self.job_id = job_id


class JobExecutionError(JobSchedulerError):
    """Raised when job execution fails."""
    
    def __init__(
        self, 
        job_id: str, 
        message: str,
        exit_code: Optional[int] = None,
        stderr: Optional[str] = None,
        execution_time: Optional[float] = None
    ):
        details = {
            "job_id": job_id,
            "exit_code": exit_code,
            "stderr": stderr,
            "execution_time": execution_time
        }
        super().__init__(
            message, 
            error_code="EXECUTION_ERROR", 
            details=details
        )
        self.job_id = job_id
        self.exit_code = exit_code
        self.stderr = stderr


class JobTimeoutError(JobExecutionError):
    """Raised when job execution exceeds timeout limit."""
    
    def __init__(
        self, 
        job_id: str, 
        timeout_seconds: float,
        message: Optional[str] = None
    ):
        message = message or f"Job '{job_id}' exceeded timeout of {timeout_seconds}s"
        super().__init__(
            job_id=job_id,
            message=message,
            execution_time=timeout_seconds
        )
        self.error_code = "EXECUTION_TIMEOUT"
        self.timeout_seconds = timeout_seconds


class SecurityError(JobSchedulerError):
    """Base class for security-related errors."""
    
    def __init__(
        self, 
        message: str, 
        user_id: Optional[str] = None,
        resource: Optional[str] = None
    ):
        details = {
            "user_id": user_id,
            "resource": resource
        }
        super().__init__(
            message, 
            error_code="SECURITY_ERROR", 
            details=details
        )
        self.user_id = user_id
        self.resource = resource


class AuthenticationError(SecurityError):
    """Raised when authentication fails."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)
        self.error_code = "AUTHENTICATION_ERROR"


class AuthorizationError(SecurityError):
    """Raised when user lacks required permissions."""
    
    def __init__(
        self, 
        message: str, 
        user_id: Optional[str] = None,
        required_permission: Optional[str] = None,
        resource: Optional[str] = None
    ):
        super().__init__(message, user_id, resource)
        self.error_code = "AUTHORIZATION_ERROR"
        self.details["required_permission"] = required_permission
        self.required_permission = required_permission


class RateLimitExceededError(SecurityError):
    """Raised when rate limit is exceeded."""
    
    def __init__(
        self, 
        user_id: str, 
        limit: int, 
        window_seconds: int,
        current_count: int
    ):
        message = (
            f"Rate limit exceeded for user '{user_id}': "
            f"{current_count}/{limit} requests in {window_seconds}s window"
        )
        super().__init__(message, user_id)
        self.error_code = "RATE_LIMIT_EXCEEDED"
        self.details.update({
            "limit": limit,
            "window_seconds": window_seconds,
            "current_count": current_count
        })
        self.limit = limit
        self.window_seconds = window_seconds
        self.current_count = current_count


class ResourceExhaustedError(JobSchedulerError):
    """Raised when system resources are exhausted."""
    
    def __init__(
        self, 
        resource_type: str, 
        current_usage: float,
        limit: float,
        message: Optional[str] = None
    ):
        message = message or (
            f"Resource '{resource_type}' exhausted: "
            f"{current_usage:.2f}/{limit:.2f}"
        )
        details = {
            "resource_type": resource_type,
            "current_usage": current_usage,
            "limit": limit,
            "usage_percentage": (current_usage / limit) * 100
        }
        super().__init__(
            message, 
            error_code="RESOURCE_EXHAUSTED", 
            details=details
        )
        self.resource_type = resource_type
        self.current_usage = current_usage
        self.limit = limit


class JobConcurrencyError(JobSchedulerError):
    """Raised when job concurrency limits are exceeded."""
    
    def __init__(
        self, 
        job_type: str, 
        current_jobs: int, 
        max_concurrent: int
    ):
        message = (
            f"Concurrency limit exceeded for job type '{job_type}': "
            f"{current_jobs}/{max_concurrent} jobs running"
        )
        super().__init__(
            message, 
            error_code="CONCURRENCY_LIMIT_EXCEEDED",
            details={
                "job_type": job_type,
                "current_jobs": current_jobs,
                "max_concurrent": max_concurrent
            }
        )
        self.job_type = job_type
        self.current_jobs = current_jobs
        self.max_concurrent = max_concurrent


class ConfigurationError(JobSchedulerError):
    """Raised when there are configuration-related errors."""
    
    def __init__(
        self, 
        message: str, 
        config_key: Optional[str] = None,
        config_value: Any = None
    ):
        details = {
            "config_key": config_key,
            "config_value": str(config_value) if config_value is not None else None
        }
        super().__init__(
            message, 
            error_code="CONFIGURATION_ERROR", 
            details=details
        )
        self.config_key = config_key
        self.config_value = config_value


class StorageError(JobSchedulerError):
    """Raised when database or storage operations fail."""
    
    def __init__(
        self, 
        message: str, 
        operation: Optional[str] = None,
        table: Optional[str] = None
    ):
        details = {
            "operation": operation,
            "table": table
        }
        super().__init__(
            message, 
            error_code="STORAGE_ERROR", 
            details=details
        )
        self.operation = operation
        self.table = table


# Exception mapping for HTTP status codes
EXCEPTION_HTTP_STATUS_MAP = {
    JobValidationError: 400,
    JobNotFoundError: 404,
    AuthenticationError: 401,
    AuthorizationError: 403,
    RateLimitExceededError: 429,
    ResourceExhaustedError: 503,
    JobConcurrencyError: 503,
    ConfigurationError: 500,
    StorageError: 500,
    JobExecutionError: 500,
    JobTimeoutError: 500,
    JobSchedulerError: 500,  # Default fallback
}