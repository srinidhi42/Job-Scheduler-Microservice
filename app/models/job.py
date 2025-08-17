from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from . import Base

class Job(Base):
    """Model representing a job that can be scheduled and executed."""
    
    __tablename__ = "jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    command = Column(Text, nullable=False)  # Command to execute (shell command, API endpoint, etc.)
    max_retries = Column(Integer, default=0)
    timeout_seconds = Column(Integer, default=3600)  # Default 1 hour timeout
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    schedules = relationship("Schedule", back_populates="job", cascade="all, delete-orphan")
    executions = relationship("Execution", back_populates="job", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Job(id={self.id}, name='{self.name}')>"