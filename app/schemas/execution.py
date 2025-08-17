from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from models.execution import ExecutionStatus

class ExecutionBase(BaseModel):
    """Base schema with common execution attributes."""
    job_id: int = Field(..., description="ID of the job")
    schedule_id: Optional[int] = Field(None, description="ID of the schedule that triggered this execution")
    status: ExecutionStatus = Field(ExecutionStatus.PENDING, description="Current status of the execution")
    scheduled_time: Optional[datetime] = Field(None, description="When the execution was scheduled to run")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional execution metadata")

class ExecutionCreate(ExecutionBase):
    """Schema for creating a new execution."""
    pass

class ExecutionUpdate(BaseModel):
    """Schema for updating an existing execution."""
    status: Optional[ExecutionStatus] = Field(None, description="Current status of the execution")
    start_time: Optional[datetime] = Field(None, description="When the execution started")
    end_time: Optional[datetime] = Field(None, description="When the execution completed")
    exit_code: Optional[int] = Field(None, description="Exit code of the executed command")
    output: Optional[str] = Field(None, description="Standard output from the execution")
    error: Optional[str] = Field(None, description="Error output from the execution")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional execution metadata")
    retry_count: Optional[int] = Field(None, description="Number of times this execution has been retried")
    
    @validator('end_time')
    def end_time_after_start_time(cls, v, values):
        if v and 'start_time' in values and values['start_time'] and v < values['start_time']:
            raise ValueError('end_time must be after start_time')
        return v

class ExecutionResponse(BaseModel):
    """Schema for execution responses from the API."""
    id: int
    job_id: int
    schedule_id: Optional[int] = None
    status: ExecutionStatus
    scheduled_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    exit_code: Optional[int] = None
    output: Optional[str] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    retry_count: int
    retry_of: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    duration: Optional[float] = None
    
    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "job_id": 1,
                "schedule_id": 1,
                "status": "success",
                "scheduled_time": "2025-08-14T00:00:00",
                "start_time": "2025-08-14T00:00:05",
                "end_time": "2025-08-14T00:01:15",
                "exit_code": 0,
                "output": "Backup completed successfully",
                "error": None,
                "metadata": {"file_size": "250MB", "destination": "/backups/db_20250814.sql"},
                "retry_count": 0,
                "retry_of": None,
                "created_at": "2025-08-14T00:00:00",
                "updated_at": "2025-08-14T00:01:15",
                "duration": 70.0
            }
        }