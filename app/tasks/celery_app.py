import os
from celery import Celery
from celery.signals import worker_init, worker_process_init, task_failure
from kombu import Exchange, Queue
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Create the Celery app
celery_app = Celery(
    "scheduler_service",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
)

# Configure Celery
celery_app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    
    # Task execution settings
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_time_limit=3600,  # 1 hour maximum task runtime
    task_soft_time_limit=3300,  # 55 minutes soft limit
    
    # Result settings
    result_expires=86400,  # Results expire after 1 day
    
    # Concurrency settings
    worker_concurrency=os.cpu_count() or 4,
    
    # Retry settings
    task_default_retry_delay=60,  # 60 seconds
    task_max_retries=3,
    
    # Beat schedule for periodic tasks
    beat_schedule={
        "check_due_jobs": {
            "task": "tasks.scheduled_tasks.check_due_jobs",
            "schedule": 60.0,  # Every minute
            "options": {"queue": "scheduler"}
        },
        "cleanup_old_executions": {
            "task": "tasks.scheduled_tasks.cleanup_old_executions",
            "schedule": 86400.0,  # Once a day
            "options": {"queue": "maintenance"}
        },
        "health_check": {
            "task": "tasks.scheduled_tasks.health_check",
            "schedule": 300.0,  # Every 5 minutes
            "options": {"queue": "maintenance"}
        }
    },
    
    # Task queues
    task_queues=(
        Queue("default", Exchange("default"), routing_key="default"),
        Queue("jobs", Exchange("jobs"), routing_key="jobs.#"),
        Queue("scheduler", Exchange("scheduler"), routing_key="scheduler.#"),
        Queue("maintenance", Exchange("maintenance"), routing_key="maintenance.#"),
        Queue("high_priority", Exchange("high_priority"), routing_key="high_priority.#"),
    ),
    
    # Task routing
    task_routes={
        "tasks.job_tasks.execute_job": {"queue": "jobs"},
        "tasks.job_tasks.process_job_result": {"queue": "jobs"},
        "tasks.job_tasks.handle_job_retry": {"queue": "high_priority"},
        "tasks.scheduled_tasks.check_due_jobs": {"queue": "scheduler"},
        "tasks.scheduled_tasks.cleanup_old_executions": {"queue": "maintenance"},
        "tasks.scheduled_tasks.health_check": {"queue": "maintenance"},
    }
)

# Import Celery tasks to ensure they're registered
from . import job_tasks, scheduled_tasks

# Database session setup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from models import Base

# Create a database engine and session factory when the worker starts
engine = None
SessionFactory = None

@worker_init.connect
def setup_database(sender=None, conf=None, **kwargs):
    """Set up database connection when worker initializes."""
    global engine, SessionFactory
    
    # Get database URL from environment variable
    database_url = os.getenv("DATABASE_URL", "sqlite:///scheduler.db")
    
    # Create engine and session factory
    engine = create_engine(database_url)
    SessionFactory = scoped_session(sessionmaker(bind=engine))
    
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    
    logger.info("Database initialized for Celery worker")

@worker_process_init.connect
def setup_worker_process(sender=None, **kwargs):
    """Set up resources for each worker process."""
    global engine, SessionFactory
    
    # Close any existing connections from the parent process
    if engine:
        engine.dispose()
    
    # Re-create the engine and session factory
    database_url = os.getenv("DATABASE_URL", "sqlite:///scheduler.db")
    engine = create_engine(database_url)
    SessionFactory = scoped_session(sessionmaker(bind=engine))
    
    logger.info("Worker process initialized")

@task_failure.connect
def handle_task_failure(sender=None, task_id=None, exception=None, args=None, kwargs=None, traceback=None, einfo=None, **kw):
    """Log task failures."""
    logger.error(f"Task {task_id} failed: {exception}")
    logger.error(f"Task args: {args}, kwargs: {kwargs}")
    logger.error(f"Traceback: {einfo}")

def get_db_session():
    """Get a database session for use in a task."""
    if SessionFactory is None:
        setup_database()
    return SessionFactory()