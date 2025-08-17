import os
from typing import Generator, Optional
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import logging

logger = logging.getLogger(__name__)

# Get database URL from environment variable with fallback
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///scheduler.db")

# Configure connection pool
if DATABASE_URL.startswith("sqlite"):
    # SQLite-specific configuration
    connect_args = {"check_same_thread": False}
    poolclass = None
    pool_size = None
    max_overflow = None
    pool_timeout = None
    pool_recycle = None
    pool_pre_ping = True
else:
    # PostgreSQL, MySQL, etc. configuration
    connect_args = {}
    poolclass = None
    pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
    max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    pool_recycle = int(os.getenv("DB_POOL_RECYCLE", "1800"))  # 30 minutes
    pool_pre_ping = True

# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    poolclass=poolclass,
    pool_size=pool_size,
    max_overflow=max_overflow,
    pool_timeout=pool_timeout,
    pool_recycle=pool_recycle,
    pool_pre_ping=pool_pre_ping,
    echo=os.getenv("DB_ECHO", "false").lower() == "true"
)

# Enable foreign key support for SQLite
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db() -> Generator[Session, None, None]:
    """
    Dependency for FastAPI to get a database session.
    
    Usage:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db)):
            # Use db session here
            pass
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db() -> None:
    """Initialize database by creating all tables."""
    from .base import Base
    
    try:
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise

class DBSessionManager:
    """
    Context manager for database sessions.
    
    Usage:
        with DBSessionManager() as db:
            # Use db session here
            pass
    """
    
    def __init__(self):
        self.db = None
    
    def __enter__(self) -> Session:
        self.db = SessionLocal()
        return self.db
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db:
            if exc_type:
                self.db.rollback()
            self.db.close()