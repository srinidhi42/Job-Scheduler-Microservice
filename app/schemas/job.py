from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator

class JobBase(BaseModel):
    """Base schema with common job attributes."""
    name: str = Field(..., description="Name of the job", example="database-backup")
    description: Optional[str] = Field(None, description="Detailed description of the job")
    command: str = Field(..., description="Command to execute", example="pg_dump -U postgres mydb > /backups/db.sql")
    max_retries: int = Field(0, description="Maximum number of retries on failure", ge=0)
    timeout_seconds: int = Field(3600, description="Execution timeout in seconds", ge=1)
    active: bool = Field(True, description="Whether the job is active")

class JobCreate(JobBase):
    """Schema for creating a new job."""
    pass

class JobUpdate(BaseModel):
    """Schema for updating an existing job."""
    name: Optional[str] = Field(None, description="Name of the job")
    description: Optional[str] = Field(None, description="Detailed description of the job")
    command: Optional[str] = Field(None, description="Command to execute")
    max_retries: Optional[int] = Field(None, description="Maximum number of retries on failure", ge=0)
    timeout_seconds: Optional[int] = Field(None, description="Execution timeout in seconds", ge=1)
    active: Optional[bool] = Field(None, description="Whether the job is active")

class JobResponse(JobBase):
    """Schema for job responses from the API."""
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "name": "database-backup",
                "description": "Daily backup of the production database",
                "command": "pg_dump -U postgres mydb > /backups/db.sql",
                "max_retries": 3,
                "timeout_seconds": 7200,
                "active": True,
                "created_at": "2025-08-14T10:00:00",
                "updated_at": "2025-08-14T10:00:00"
            }
        }