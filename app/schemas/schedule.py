from typing import Optional, List, Dict, Any, Union, Literal
from datetime import datetime
from pydantic import BaseModel, Field, validator, root_validator
from models.schedule import ScheduleType

class ScheduleBase(BaseModel):
    """Base schema with common schedule attributes."""
    job_id: int = Field(..., description="ID of the job to schedule")
    type: ScheduleType = Field(..., description="Type of schedule")
    timezone: str = Field("UTC", description="Timezone for schedule calculations")
    start_date: Optional[datetime] = Field(None, description="When this schedule becomes active")
    end_date: Optional[datetime] = Field(None, description="When this schedule expires")
    active: bool = Field(True, description="Whether the schedule is active")

    @validator('end_date')
    def end_date_after_start_date(cls, v, values):
        if v and 'start_date' in values and values['start_date'] and v < values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v

class CronScheduleCreate(ScheduleBase):
    """Schema for creating a cron-based schedule."""
    type: Literal[ScheduleType.CRON] = ScheduleType.CRON
    cron_expression: str = Field(..., description="Cron expression for schedule", example="0 0 * * *")

class IntervalScheduleCreate(ScheduleBase):
    """Schema for creating an interval-based schedule."""
    type: Literal[ScheduleType.INTERVAL] = ScheduleType.INTERVAL
    interval_seconds: int = Field(..., description="Interval in seconds between executions", example=3600, gt=0)

class OneTimeScheduleCreate(ScheduleBase):
    """Schema for creating a one-time schedule."""
    type: Literal[ScheduleType.ONE_TIME] = ScheduleType.ONE_TIME
    scheduled_time: datetime = Field(..., description="When to execute the job")

# Union type for creating any schedule
ScheduleCreate = Union[CronScheduleCreate, IntervalScheduleCreate, OneTimeScheduleCreate]

class ScheduleUpdate(BaseModel):
    """Schema for updating an existing schedule."""
    job_id: Optional[int] = Field(None, description="ID of the job to schedule")
    timezone: Optional[str] = Field(None, description="Timezone for schedule calculations")
    start_date: Optional[datetime] = Field(None, description="When this schedule becomes active")
    end_date: Optional[datetime] = Field(None, description="When this schedule expires")
    active: Optional[bool] = Field(None, description="Whether the schedule is active")
    
    # Type-specific fields
    cron_expression: Optional[str] = Field(None, description="Cron expression for schedule")
    interval_seconds: Optional[int] = Field(None, description="Interval in seconds between executions", gt=0)
    scheduled_time: Optional[datetime] = Field(None, description="When to execute the job (for one-time)")
    
    @root_validator
    def validate_type_specific_fields(cls, values):
        # This validation will be performed in the API layer since we need to know
        # the current schedule type, which we don't have in this update schema
        return values

class ScheduleResponse(BaseModel):
    """Schema for schedule responses from the API."""
    id: int
    job_id: int
    type: ScheduleType
    
    # Type-specific fields
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    scheduled_time: Optional[datetime] = None
    
    # Common fields
    timezone: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    active: bool
    last_scheduled_time: Optional[datetime] = None
    next_scheduled_time: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "job_id": 1,
                "type": "cron",
                "cron_expression": "0 0 * * *",
                "interval_seconds": None,
                "scheduled_time": None,
                "timezone": "UTC",
                "start_date": "2025-08-14T00:00:00",
                "end_date": None,
                "active": True,
                "last_scheduled_time": "2025-08-14T00:00:00",
                "next_scheduled_time": "2025-08-15T00:00:00",
                "created_at": "2025-08-14T10:00:00",
                "updated_at": "2025-08-14T10:00:00"
            }
        }