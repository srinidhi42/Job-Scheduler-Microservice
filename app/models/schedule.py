import enum
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from . import Base

class ScheduleType(enum.Enum):
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"

class Schedule(Base):
    """Model representing a schedule for when a job should be executed."""
    
    __tablename__ = "schedules"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    type = Column(Enum(ScheduleType), nullable=False)
    
    # For CRON schedules
    cron_expression = Column(String(100), nullable=True)
    
    # For INTERVAL schedules (in seconds)
    interval_seconds = Column(Integer, nullable=True)
    
    # For ONE_TIME schedules
    scheduled_time = Column(DateTime, nullable=True)
    
    # Common fields
    timezone = Column(String(50), default="UTC")
    start_date = Column(DateTime, nullable=True)  # When this schedule becomes active
    end_date = Column(DateTime, nullable=True)    # When this schedule expires
    active = Column(Boolean, default=True)
    last_scheduled_time = Column(DateTime, nullable=True)  # When was the last time this schedule triggered
    next_scheduled_time = Column(DateTime, nullable=True)  # When is the next time this schedule will trigger
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    job = relationship("Job", back_populates="schedules")
    
    def __repr__(self):
        return f"<Schedule(id={self.id}, type='{self.type.value}', job_id={self.job_id})>"