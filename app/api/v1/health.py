# app/api/v1/health.py

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, Any, List
import asyncio
import psutil
import aioredis
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_db, get_redis, get_settings
from app.core.config import Settings
from app.schemas.health import (
    HealthCheckResponse,
    ServiceStatus,
    DatabaseStatus,
    RedisStatus,
    QueueStatus,
    SystemMetrics
)
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/health",
    tags=["health"],
    responses={
        503: {"description": "Service Unavailable"}
    }
)


async def check_database_health(db: AsyncSession) -> DatabaseStatus:
    """Check database connectivity and performance."""
    try:
        start_time = datetime.now(timezone.utc)
        
        # Execute a simple query to test connection
        result = await db.execute(text("SELECT 1"))
        await db.commit()
        
        # Check connection pool stats
        pool_status = db.bind.pool.status() if hasattr(db.bind, 'pool') else "N/A"
        
        response_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        # Get database statistics
        stats_query = text("""
            SELECT 
                (SELECT count(*) FROM jobs) as total_jobs,
                (SELECT count(*) FROM jobs WHERE is_active = true) as active_jobs,
                (SELECT count(*) FROM job_executions WHERE created_at > NOW() - INTERVAL '1 hour') as recent_executions
        """)
        stats_result = await db.execute(stats_query)
        stats = stats_result.first()
        
        return DatabaseStatus(
            status="healthy",
            response_time_ms=response_time,
            pool_status=str(pool_status),
            statistics={
                "total_jobs": stats.total_jobs if stats else 0,
                "active_jobs": stats.active_jobs if stats else 0,
                "recent_executions": stats.recent_executions if stats else 0
            }
        )
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        return DatabaseStatus(
            status="unhealthy",
            response_time_ms=0,
            error=str(e)
        )


async def check_redis_health(redis_client: aioredis.Redis) -> RedisStatus:
    """Check Redis connectivity and performance."""
    try:
        start_time = datetime.now(timezone.utc)
        
        # Ping Redis
        await redis_client.ping()
        
        # Get Redis info
        info = await redis_client.info()
        
        response_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        # Get some key statistics
        memory_used = info.get("used_memory_human", "N/A")
        connected_clients = info.get("connected_clients", 0)
        
        return RedisStatus(
            status="healthy",
            response_time_ms=response_time,
            memory_usage=memory_used,
            connected_clients=connected_clients,
            version=info.get("redis_version", "unknown")
        )
    except Exception as e:
        logger.error(f"Redis health check failed: {str(e)}")
        return RedisStatus(
            status="unhealthy",
            response_time_ms=0,
            error=str(e)
        )


async def check_queue_health(settings: Settings) -> QueueStatus:
    """Check message queue (RabbitMQ) health."""
    try:
        import aio_pika
        
        start_time = datetime.now(timezone.utc)
        
        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        channel = await connection.channel()
        
        # Check queue statistics
        queue = await channel.declare_queue("job_queue", durable=True)
        message_count = queue.declaration_result.message_count
        
        await connection.close()
        
        response_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        return QueueStatus(
            status="healthy",
            response_time_ms=response_time,
            queue_depth=message_count,
            consumer_count=queue.declaration_result.consumer_count
        )
    except Exception as e:
        logger.error(f"Queue health check failed: {str(e)}")
        return QueueStatus(
            status="unhealthy",
            response_time_ms=0,
            error=str(e)
        )


def get_system_metrics() -> SystemMetrics:
    """Get system-level metrics."""
    try:
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)
        cpu_count = psutil.cpu_count()
        
        # Memory metrics
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_available_gb = memory.available / (1024 ** 3)
        
        # Disk metrics
        disk = psutil.disk_usage('/')
        disk_percent = disk.percent
        disk_free_gb = disk.free / (1024 ** 3)
        
        # Process metrics
        process = psutil.Process()
        process_memory_mb = process.memory_info().rss / (1024 ** 2)
        process_cpu_percent = process.cpu_percent(interval=0.1)
        
        return SystemMetrics(
            cpu_usage_percent=cpu_percent,
            cpu_count=cpu_count,
            memory_usage_percent=memory_percent,
            memory_available_gb=round(memory_available_gb, 2),
            disk_usage_percent=disk_percent,
            disk_free_gb=round(disk_free_gb, 2),
            process_memory_mb=round(process_memory_mb, 2),
            process_cpu_percent=process_cpu_percent
        )
    except Exception as e:
        logger.error(f"Failed to get system metrics: {str(e)}")
        return SystemMetrics()


@router.get(
    "/",
    response_model=HealthCheckResponse,
    summary="Comprehensive health check",
    description="Check the health of all system components"
)
async def health_check(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
    settings: Settings = Depends(get_settings)
) -> HealthCheckResponse:
    """
    Comprehensive health check endpoint.
    
    This endpoint checks:
    - API service status
    - Database connectivity and performance
    - Redis cache connectivity
    - Message queue (RabbitMQ) connectivity
    - System resource usage
    
    Returns HTTP 503 if any critical component is unhealthy.
    """
    try:
        # Run all health checks in parallel
        db_check_task = check_database_health(db)
        redis_check_task = check_redis_health(redis)
        queue_check_task = check_queue_health(settings)
        
        db_status, redis_status, queue_status = await asyncio.gather(
            db_check_task,
            redis_check_task,
            queue_check_task
        )
        
        # Get system metrics
        system_metrics = get_system_metrics()
        
        # Determine overall health
        is_healthy = all([
            db_status.status == "healthy",
            redis_status.status == "healthy",
            queue_status.status == "healthy"
        ])
        
        overall_status = "healthy" if is_healthy else "degraded"
        
        response = HealthCheckResponse(
            status=overall_status,
            timestamp=datetime.now(timezone.utc),
            service="job-scheduler",
            version="1.0.0",
            uptime_seconds=int((datetime.now(timezone.utc) - settings.START_TIME).total_seconds()),
            components={
                "database": db_status,
                "cache": redis_status,
                "queue": queue_status
            },
            metrics=system_metrics
        )
        
        if not is_healthy:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=response.dict()
            )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )


@router.get(
    "/live",
    response_model=ServiceStatus,
    summary="Liveness probe",
    description="Simple liveness check for Kubernetes"
)
async def liveness() -> ServiceStatus:
    """
    Kubernetes liveness probe endpoint.
    
    This endpoint performs a simple check to verify the service is running.
    It does not check external dependencies.
    
    Used by Kubernetes to determine if the pod should be restarted.
    """
    return ServiceStatus(
        status="healthy",
        service="job-scheduler",
        timestamp=datetime.now(timezone.utc)
    )


@router.get(
    "/ready",
    response_model=ServiceStatus,
    summary="Readiness probe",
    description="Readiness check for Kubernetes"
)
async def readiness(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis)
) -> ServiceStatus:
    """
    Kubernetes readiness probe endpoint.
    
    This endpoint checks if the service is ready to accept traffic by
    verifying critical dependencies are available.
    
    Used by Kubernetes to determine if the pod should receive traffic.
    """
    try:
        # Quick database check
        await db.execute(text("SELECT 1"))
        
        # Quick Redis check
        await redis.ping()
        
        return ServiceStatus(
            status="healthy",
            service="job-scheduler",
            timestamp=datetime.now(timezone.utc),
            details={
                "database": "connected",
                "cache": "connected"
            }
        )
        
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status": "not_ready",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )


@router.get(
    "/metrics",
    summary="Prometheus metrics",
    description="Metrics endpoint for Prometheus scraping",
    response_class=PlainTextResponse
)
async def metrics(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis)
) -> str:
    """
    Prometheus metrics endpoint.
    
    Exposes application metrics in Prometheus format for monitoring.
    """
    try:
        # Collect metrics
        metrics_lines = []
        
        # System metrics
        system_metrics = get_system_metrics()
        metrics_lines.extend([
            f"# HELP job_scheduler_cpu_usage_percent CPU usage percentage",
            f"# TYPE job_scheduler_cpu_usage_percent gauge",
            f"job_scheduler_cpu_usage_percent {system_metrics.cpu_usage_percent}",
            "",
            f"# HELP job_scheduler_memory_usage_percent Memory usage percentage",
            f"# TYPE job_scheduler_memory_usage_percent gauge",
            f"job_scheduler_memory_usage_percent {system_metrics.memory_usage_percent}",
            "",
            f"# HELP job_scheduler_process_memory_mb Process memory usage in MB",
            f"# TYPE job_scheduler_process_memory_mb gauge",
            f"job_scheduler_process_memory_mb {system_metrics.process_memory_mb}",
            ""
        ])
        
        # Database metrics
        stats_query = text("""
            SELECT 
                (SELECT count(*) FROM jobs) as total_jobs,
                (SELECT count(*) FROM jobs WHERE is_active = true) as active_jobs,
                (SELECT count(*) FROM job_executions WHERE status = 'running') as running_jobs,
                (SELECT count(*) FROM job_executions WHERE status = 'failed' AND created_at > NOW() - INTERVAL '1 hour') as failed_jobs_1h
        """)
        result = await db.execute(stats_query)
        stats = result.first()
        
        if stats:
            metrics_lines.extend([
                f"# HELP job_scheduler_total_jobs Total number of jobs",
                f"# TYPE job_scheduler_total_jobs gauge",
                f"job_scheduler_total_jobs {stats.total_jobs}",
                "",
                f"# HELP job_scheduler_active_jobs Number of active jobs",
                f"# TYPE job_scheduler_active_jobs gauge",
                f"job_scheduler_active_jobs {stats.active_jobs}",
                "",
                f"# HELP job_scheduler_running_jobs Number of currently running jobs",
                f"# TYPE job_scheduler_running_jobs gauge",
                f"job_scheduler_running_jobs {stats.running_jobs}",
                "",
                f"# HELP job_scheduler_failed_jobs_1h Number of failed jobs in the last hour",
                f"# TYPE job_scheduler_failed_jobs_1h counter",
                f"job_scheduler_failed_jobs_1h {stats.failed_jobs_1h}",
                ""
            ])
        
        # Redis metrics
        info = await redis.info()
        metrics_lines.extend([
            f"# HELP job_scheduler_redis_connected_clients Number of connected Redis clients",
            f"# TYPE job_scheduler_redis_connected_clients gauge",
            f"job_scheduler_redis_connected_clients {info.get('connected_clients', 0)}",
            "",
            f"# HELP job_scheduler_redis_used_memory_bytes Redis memory usage in bytes",
            f"# TYPE job_scheduler_redis_used_memory_bytes gauge",
            f"job_scheduler_redis_used_memory_bytes {info.get('used_memory', 0)}",
            ""
        ])
        
        return "\n".join(metrics_lines)
        
    except Exception as e:
        logger.error(f"Failed to generate metrics: {str(e)}")
        return f"# Error generating metrics: {str(e)}"


# Additional endpoints for debugging (only in development)
if settings.DEBUG:
    @router.get(
        "/debug",
        summary="Debug information",
        description="Detailed debug information (only available in debug mode)"
    )
    async def debug_info(
        db: AsyncSession = Depends(get_db),
        redis: aioredis.Redis = Depends(get_redis),
        settings: Settings = Depends(get_settings)
    ) -> Dict[str, Any]:
        """Get detailed debug information about the system."""
        
        # Get all environment variables (filtered)
        safe_env_vars = {
            k: v for k, v in settings.dict().items()
            if not any(secret in k.lower() for secret in ["password", "secret", "key", "token"])
        }
        
        # Get database connection info
        db_info = {
            "url": settings.DATABASE_URL.split("@")[-1],  # Hide credentials
            "pool_size": settings.DATABASE_POOL_SIZE,
            "max_overflow": settings.DATABASE_MAX_OVERFLOW
        }
        
        # Get Redis info
        redis_info = await redis.info()
        
        return {
            "environment": settings.ENVIRONMENT,
            "debug_mode": settings.DEBUG,
            "start_time": settings.START_TIME.isoformat(),
            "environment_variables": safe_env_vars,
            "database": db_info,
            "redis": {
                "version": redis_info.get("redis_version"),
                "memory_human": redis_info.get("used_memory_human"),
                "connected_clients": redis_info.get("connected_clients")
            },
            "system": {
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "cpu_count": psutil.cpu_count(),
                "memory_total_gb": round(psutil.virtual_memory().total / (1024 ** 3), 2)
            }
        }


# Import additional dependencies for metrics
from fastapi.responses import PlainTextResponse
import platform