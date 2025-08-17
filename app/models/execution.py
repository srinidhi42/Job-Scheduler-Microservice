import enum
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Enum, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from . import Base

class ExecutionStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

class Execution(Base):
    """Model representing a single execution instance of a job."""
    
    __tablename__ = "executions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    schedule_id = Column(Integer, ForeignKey("schedules.id"), nullable=True)  # Can be null for manually triggered executions
    
    status = Column(Enum(ExecutionStatus), default=ExecutionStatus.PENDING, nullable=False)
    scheduled_time = Column(DateTime, nullable=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    
    # Runtime data
    exit_code = Column(Integer, nullable=True)
    output = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    metadata = Column(JSON, nullable=True)  # For any additional execution-specific data
    
    # Retry information
    retry_count = Column(Integer, default=0)
    retry_of = Column(Integer, ForeignKey("executions.id"), nullable=True)  # If this is a retry, reference the original execution
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    job = relationship("Job", back_populates="executions")
    schedule = relationship("Schedule", backref="executions")
    retries = relationship("Execution", backref=relationship.backref("original", remote_side=[id]))
    
    def __repr__(self):
        return f"<Execution(id={self.id}, job_id={self.job_id}, status='{self.status.value}')>"
    
    def duration(self):
        """Calculate the execution duration in seconds, if applicable."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None