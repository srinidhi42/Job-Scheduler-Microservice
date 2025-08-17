"""Test fixtures and configuration for the Job Scheduler Microservice."""

import asyncio
import os
import random
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Callable, Dict, Generator, List, Optional

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

# Import application modules
from db.base import Base
from db.session import get_db
from main import create_app
from models.execution import Execution, ExecutionStatus
from models.job import Job
from models.schedule import Schedule, ScheduleType
from services.executor_service import ExecutorService
from services.job_service import JobService
from services.scheduler_service import SchedulerService
from tasks.celery_app import celery_app

# ===============================
# Database Fixtures
# ===============================

@pytest.fixture(scope="session")
def engine():
    """Create a SQLAlchemy engine for testing."""
    # Use in-memory SQLite for tests
    test_db_url = os.environ.get("TEST_DATABASE_URL", "sqlite:///:memory:")
    
    engine = create_engine(
        test_db_url,
        connect_args={"check_same_thread": False} if test_db_url.startswith("sqlite") else {},
        poolclass=StaticPool,
        echo=False
    )
    
    # Enable foreign key support for SQLite
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        if test_db_url.startswith("sqlite"):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    yield engine
    
    # Drop all tables after tests
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session(engine):
    """Create a new database session for a test."""
    # Connect to the database
    connection = engine.connect()
    # Begin a non-ORM transaction
    transaction = connection.begin()
    # Bind a session to the connection
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=connection)
    session = TestSessionLocal()
    
    yield session
    
    # Roll back the transaction and close the session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def override_get_db(db_session):
    """Override the get_db dependency for testing."""
    def _override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    return _override_get_db


# ===============================
# Application Fixtures
# ===============================

@pytest.fixture(scope="function")
def app(override_get_db):
    """Create a FastAPI application for testing."""
    app = create_app()
    
    # Override the get_db dependency
    app.dependency_overrides[get_db] = override_get_db
    
    return app


@pytest.fixture(scope="function")
def client(app):
    """Create a test client for the application."""
    with TestClient(app) as client:
        yield client


# ===============================
# Service Fixtures
# ===============================

@pytest.fixture(scope="function")
def job_service(db_session):
    """Create a JobService instance for testing."""
    return JobService(db_session)


@pytest.fixture(scope="function")
def scheduler_service(db_session):
    """Create a SchedulerService instance for testing."""
    return SchedulerService(db_session)


@pytest.fixture(scope="function")
def executor_service(db_session):
    """Create an ExecutorService instance for testing."""
    return ExecutorService(db_session)


# ===============================
# Model Factory Fixtures
# ===============================

@pytest.fixture
def create_job(db_session):
    """Factory fixture to create a job for testing."""
    created_jobs = []
    
    def _create_job(**kwargs):
        """Create a job with default values."""
        job_data = {
            "name": f"test-job-{random.randint(1, 10000)}",
            "description": "Test job for unit tests",
            "command": "echo 'Hello, World!'",
            "max_retries": 3,
            "timeout_seconds": 60,
            "active": True,
        }
        job_data.update(kwargs)
        
        job = Job(**job_data)
        db_session.add(job)
        db_session.commit()
        db_session.refresh(job)
        
        created_jobs.append(job)
        return job
    
    yield _create_job
    
    # Clean up created jobs
    for job in created_jobs:
        db_session.delete(job)
    db_session.commit()


@pytest.fixture
def create_schedule(db_session, create_job):
    """Factory fixture to create a schedule for testing."""
    created_schedules = []
    
    def _create_schedule(**kwargs):
        """Create a schedule with default values."""
        # Create a job if not provided
        if "job_id" not in kwargs:
            job = create_job()
            kwargs["job_id"] = job.id
        
        schedule_type = kwargs.pop("type", ScheduleType.INTERVAL)
        
        schedule_data = {
            "type": schedule_type,
            "active": True,
            "timezone": "UTC",
        }
        
        # Add type-specific defaults
        if schedule_type == ScheduleType.CRON:
            schedule_data["cron_expression"] = kwargs.pop("cron_expression", "*/5 * * * *")
        elif schedule_type == ScheduleType.INTERVAL:
            schedule_data["interval_seconds"] = kwargs.pop("interval_seconds", 300)
        elif schedule_type == ScheduleType.ONE_TIME:
            schedule_data["scheduled_time"] = kwargs.pop("scheduled_time", datetime.utcnow() + timedelta(minutes=5))
        
        schedule_data.update(kwargs)
        
        schedule = Schedule(**schedule_data)
        db_session.add(schedule)
        db_session.commit()
        db_session.refresh(schedule)
        
        # Calculate next run time
        if "next_scheduled_time" not in kwargs:
            scheduler_service = SchedulerService(db_session)
            scheduler_service._calculate_next_run(schedule)
            db_session.commit()
            db_session.refresh(schedule)
        
        created_schedules.append(schedule)
        return schedule
    
    yield _create_schedule
    
    # Clean up created schedules
    for schedule in created_schedules:
        db_session.delete(schedule)
    db_session.commit()


@pytest.fixture
def create_execution(db_session, create_job):
    """Factory fixture to create an execution for testing."""
    created_executions = []
    
    def _create_execution(**kwargs):
        """Create an execution with default values."""
        # Create a job if not provided
        if "job_id" not in kwargs:
            job = create_job()
            kwargs["job_id"] = job.id
        
        execution_data = {
            "status": ExecutionStatus.PENDING,
            "scheduled_time": datetime.utcnow(),
            "retry_count": 0,
        }
        execution_data.update(kwargs)
        
        execution = Execution(**execution_data)
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)
        
        created_executions.append(execution)
        return execution
    
    yield _create_execution
    
    # Clean up created executions
    for execution in created_executions:
        db_session.delete(execution)
    db_session.commit()


# ===============================
# Mock Fixtures
# ===============================

@pytest.fixture
def mock_redis(monkeypatch):
    """Mock Redis for testing."""
    class MockRedis:
        def __init__(self):
            self.data = {}
            self.expirations = {}
        
        def get(self, key):
            return self.data.get(key)
        
        def set(self, key, value, ex=None):
            self.data[key] = value
            if ex:
                self.expirations[key] = ex
            return True
        
        def setex(self, key, time, value):
            self.data[key] = value
            self.expirations[key] = time
            return True
        
        def delete(self, *keys):
            count = 0
            for key in keys:
                if key in self.data:
                    del self.data[key]
                    count += 1
                    if key in self.expirations:
                        del self.expirations[key]
            return count
        
        def exists(self, key):
            return key in self.data
        
        def ttl(self, key):
            return self.expirations.get(key, -1)
        
        def keys(self, pattern):
            import fnmatch
            return [k for k in self.data.keys() if fnmatch.fnmatch(k, pattern)]
        
        def ping(self):
            return True
    
    # Create mock instance
    mock_redis_instance = MockRedis()
    
    # Patch Redis class
    import redis
    monkeypatch.setattr(redis, "Redis", lambda **kwargs: mock_redis_instance)
    
    # Return the mock for assertions
    return mock_redis_instance


@pytest.fixture
def mock_celery(monkeypatch):
    """Mock Celery for testing."""
    class MockTask:
        def __init__(self, task_id=None):
            self.task_id = task_id or f"task-{random.randint(1000, 9999)}"
        
        def __call__(self, *args, **kwargs):
            return self
        
        def delay(self, *args, **kwargs):
            return self
        
        def apply_async(self, *args, **kwargs):
            return self
        
        def s(self, *args, **kwargs):
            return self
        
        def get(self, timeout=None):
            return None
    
    # Patch Celery Task
    monkeypatch.setattr(celery_app, "Task", MockTask)
    
    # Patch task registration
    def mock_task(*args, **kwargs):
        def decorator(func):
            task = MockTask()
            return task
        return decorator
    
    monkeypatch.setattr(celery_app, "task", mock_task)
    
    # Return the patched app
    return celery_app


@pytest.fixture
def mock_subprocess(monkeypatch):
    """Mock subprocess for testing command execution."""
    class MockCompletedProcess:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr
    
    class MockPopen:
        def __init__(self, cmd, **kwargs):
            self.cmd = cmd
            self.returncode = 0
            self.stdout = "Mock stdout output"
            self.stderr = ""
            self.poll_count = 0
        
        def communicate(self, timeout=None):
            # Simulate timeout if requested
            if timeout == 0:
                import subprocess
                raise subprocess.TimeoutExpired(self.cmd, timeout)
            return self.stdout, self.stderr
        
        def poll(self):
            self.poll_count += 1
            # Return None a few times to simulate running process
            if self.poll_count < 3:
                return None
            return self.returncode
        
        def terminate(self):
            pass
        
        def kill(self):
            pass
    
    import subprocess
    monkeypatch.setattr(subprocess, "Popen", MockPopen)
    
    # Return a configurable function to set return values
    def configure_mock(returncode=0, stdout="Mock stdout", stderr="", timeout_on_communicate=False):
        def _configure_instance(instance):
            instance.returncode = returncode
            instance.stdout = stdout
            instance.stderr = stderr
            if timeout_on_communicate:
                orig_communicate = instance.communicate
                def communicate_timeout(*args, **kwargs):
                    import subprocess
                    raise subprocess.TimeoutExpired(instance.cmd, kwargs.get('timeout', 0))
                instance.communicate = communicate_timeout
        
        # Apply configuration to future instances
        orig_init = MockPopen.__init__
        def new_init(self, cmd, **kwargs):
            orig_init(self, cmd, **kwargs)
            _configure_instance(self)
        MockPopen.__init__ = new_init
        
        return MockPopen
    
    return configure_mock


# ===============================
# Utility Fixtures
# ===============================

@pytest.fixture
def assert_json_response():
    """Fixture to assert JSON response structure."""
    def _assert_json_response(response, status_code=200, expected_keys=None):
        assert response.status_code == status_code, f"Expected status code {status_code}, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert isinstance(data, dict), f"Expected JSON object, got {type(data)}"
        
        if expected_keys:
            for key in expected_keys:
                assert key in data, f"Expected key '{key}' not found in response: {data}"
        
        return data
    
    return _assert_json_response


@pytest.fixture
def future_datetime():
    """Generate a future datetime for testing."""
    def _future_datetime(minutes=5):
        return datetime.utcnow() + timedelta(minutes=minutes)
    
    return _future_datetime


@pytest.fixture
def past_datetime():
    """Generate a past datetime for testing."""
    def _past_datetime(minutes=5):
        return datetime.utcnow() - timedelta(minutes=minutes)
    
    return _past_datetime


# ===============================
# Async Fixtures
# ===============================

@pytest_asyncio.fixture
async def async_client(app):
    """Create an async test client for the application."""
    from httpx import AsyncClient
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def async_db_session(engine):
    """Create an async database session for testing."""
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import sessionmaker
    
    # Create a connection
    connection = engine.connect()
    transaction = connection.begin()
    
    # Create a session
    TestSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=connection, class_=AsyncSession
    )
    session = TestSessionLocal()
    
    yield session
    
    # Clean up
    await session.close()
    transaction.rollback()
    connection.close()