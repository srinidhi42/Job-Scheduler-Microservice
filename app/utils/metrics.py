import time
import os
from typing import Callable, Any, Dict, Optional, List, TypeVar, cast
import functools
from prometheus_client import (
    Counter, Gauge, Histogram, Summary, 
    CollectorRegistry, push_to_gateway, 
    start_http_server, REGISTRY,
    multiprocess, generate_latest
)
import logging

logger = logging.getLogger(__name__)

# Type variable for function signatures
F = TypeVar('F', bound=Callable[..., Any])

# Metrics configuration
METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
PUSH_GATEWAY_URL = os.getenv("PUSH_GATEWAY_URL", "")
METRICS_JOB_NAME = os.getenv("METRICS_JOB_NAME", "scheduler")
METRICS_INSTANCE = os.getenv("METRICS_INSTANCE", os.getenv("HOSTNAME", "unknown"))
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "true").lower() == "true"

# Define metrics
# Job execution metrics
JOB_EXECUTION_COUNT = Counter(
    "job_execution_count",
    "Number of job executions",
    ["job_name", "status"]
)

JOB_EXECUTION_DURATION = Histogram(
    "job_execution_duration_seconds",
    "Duration of job executions in seconds",
    ["job_name"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600)
)

# API metrics
API_REQUEST_COUNT = Counter(
    "api_request_count",
    "Number of API requests",
    ["method", "endpoint", "status"]
)

API_REQUEST_DURATION = Histogram(
    "api_request_duration_seconds",
    "Duration of API requests in seconds",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
)

# Queue metrics
QUEUE_SIZE = Gauge(
    "queue_size",
    "Number of items in the queue",
    ["queue_name"]
)

# System metrics
SYSTEM_INFO = Gauge(
    "system_info",
    "System information",
    ["name", "value"]
)

ACTIVE_JOBS = Gauge(
    "active_jobs",
    "Number of active jobs"
)

SCHEDULED_JOBS = Gauge(
    "scheduled_jobs",
    "Number of scheduled jobs"
)

def setup_metrics(
    enable_http: bool = True,
    enable_multiprocess: bool = False,
    push_gateway: bool = False,
    push_interval: int = 15,
) -> None:
    """
    Set up Prometheus metrics collection.
    
    Args:
        enable_http: Whether to start an HTTP server for metrics
        enable_multiprocess: Whether to enable multiprocess mode
        push_gateway: Whether to push metrics to a Pushgateway
        push_interval: How often to push metrics in seconds
    """
    if not METRICS_ENABLED:
        logger.info("Metrics collection is disabled")
        return
    
    try:
        # Set up multiprocess mode if needed
        if enable_multiprocess:
            logger.info("Enabling multiprocess metrics collection")
            multiprocess.MultiProcessCollector(REGISTRY)
        
        # Start HTTP server if requested
        if enable_http:
            logger.info(f"Starting metrics HTTP server on port {METRICS_PORT}")
            start_http_server(METRICS_PORT)
        
        # Set up push gateway if requested
        if push_gateway and PUSH_GATEWAY_URL:
            logger.info(f"Setting up metrics push to {PUSH_GATEWAY_URL} every {push_interval}s")
            
            def push_metrics():
                try:
                    push_to_gateway(
                        PUSH_GATEWAY_URL,
                        job=METRICS_JOB_NAME,
                        registry=REGISTRY,
                        grouping_key={"instance": METRICS_INSTANCE}
                    )
                except Exception as e:
                    logger.error(f"Failed to push metrics to gateway: {str(e)}")
            
            # Schedule regular pushing
            import threading
            
            def push_loop():
                while True:
                    push_metrics()
                    time.sleep(push_interval)
            
            thread = threading.Thread(target=push_loop, daemon=True)
            thread.start()
        
        logger.info("Metrics collection initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize metrics: {str(e)}")

def timing_metric(
    histogram: Histogram,
    labels: Optional[Dict[str, str]] = None,
) -> Callable[[F], F]:
    """
    Decorator to measure and record the execution time of a function.
    
    Args:
        histogram: The Prometheus histogram to record to
        labels: Labels to apply to the metric
        
    Returns:
        A decorator function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Skip if metrics are disabled
            if not METRICS_ENABLED:
                return func(*args, **kwargs)
            
            # Record execution time
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.time() - start_time
                # Apply labels if provided
                if labels:
                    histogram.labels(**labels).observe(duration)
                else:
                    histogram.observe(duration)
        
        return cast(F, wrapper)
    
    return decorator

def count_metric(
    counter: Counter,
    labels: Optional[Dict[str, str]] = None,
) -> Callable[[F], F]:
    """
    Decorator to count invocations of a function.
    
    Args:
        counter: The Prometheus counter to increment
        labels: Labels to apply to the metric
        
    Returns:
        A decorator function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Skip if metrics are disabled
            if not METRICS_ENABLED:
                return func(*args, **kwargs)
            
            # Increment counter
            if labels:
                counter.labels(**labels).inc()
            else:
                counter.inc()
            
            return func(*args, **kwargs)
        
        return cast(F, wrapper)
    
    return decorator

def track_errors(
    counter: Counter,
    labels: Optional[Dict[str, str]] = None,
    error_label_key: str = "status",
    error_label_value: str = "error",
) -> Callable[[F], F]:
    """
    Decorator to track errors in a function.
    
    Args:
        counter: The Prometheus counter to increment
        labels: Labels to apply to the metric
        error_label_key: Label key to use for errors
        error_label_value: Label value to use for errors
        
    Returns:
        A decorator function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Skip if metrics are disabled
            if not METRICS_ENABLED:
                return func(*args, **kwargs)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Increment error counter
                error_labels = labels.copy() if labels else {}
                error_labels[error_label_key] = error_label_value
                counter.labels(**error_labels).inc()
                raise e
        
        return cast(F, wrapper)
    
    return decorator

def track_gauge(
    gauge: Gauge,
    value_func: Callable[[Any], float],
    labels: Optional[Dict[str, str]] = None,
) -> Callable[[F], F]:
    """
    Decorator to track a gauge value based on a function's result.
    
    Args:
        gauge: The Prometheus gauge to set
        value_func: Function to extract the gauge value from the result
        labels: Labels to apply to the metric
        
    Returns:
        A decorator function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Skip if metrics are disabled
            if not METRICS_ENABLED:
                return func(*args, **kwargs)
            
            # Call the original function
            result = func(*args, **kwargs)
            
            # Update the gauge
            value = value_func(result)
            if labels:
                gauge.labels(**labels).set(value)
            else:
                gauge.set(value)
            
            return result
        
        return cast(F, wrapper)
    
    return decorator

def set_job_execution_metrics(
    job_name: str,
    status: str,
    duration: float,
) -> None:
    """
    Set job execution metrics.
    
    Args:
        job_name: Name of the job
        status: Status of the execution (success, failure, etc.)
        duration: Duration of the execution in seconds
    """
    if not METRICS_ENABLED:
        return
    
    JOB_EXECUTION_COUNT.labels(job_name=job_name, status=status).inc()
    JOB_EXECUTION_DURATION.labels(job_name=job_name).observe(duration)

def set_api_metrics(
    method: str,
    endpoint: str,
    status: int,
    duration: float,
) -> None:
    """
    Set API request metrics.
    
    Args:
        method: HTTP method (GET, POST, etc.)
        endpoint: API endpoint
        status: HTTP status code
        duration: Request duration in seconds
    """
    if not METRICS_ENABLED:
        return
    
    status_str = str(status)
    API_REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status_str).inc()
    API_REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

def set_queue_size(queue_name: str, size: int) -> None:
    """
    Set queue size metric.
    
    Args:
        queue_name: Name of the queue
        size: Size of the queue
    """
    if not METRICS_ENABLED:
        return
    
    QUEUE_SIZE.labels(queue_name=queue_name).set(size)

def set_system_info(name: str, value: str) -> None:
    """
    Set system information metric.
    
    Args:
        name: Name of the information
        value: Value of the information
    """
    if not METRICS_ENABLED:
        return
    
    SYSTEM_INFO.labels(name=name, value=value).set(1)

def get_metrics() -> bytes:
    """
    Get the current metrics in Prometheus format.
    
    Returns:
        Metrics data as bytes
    """
    if not METRICS_ENABLED:
        return b""
    
    return generate_latest(REGISTRY)