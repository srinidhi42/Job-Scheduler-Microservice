from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models.job import Job
from schemas.job import JobCreate, JobUpdate

class JobService:
    """Service for managing job operations."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_job(self, job_data: JobCreate) -> Job:
        """Create a new job."""
        job = Job(**job_data.dict())
        
        try:
            self.db.add(job)
            self.db.commit()
            self.db.refresh(job)
            return job
        except IntegrityError:
            self.db.rollback()
            raise ValueError(f"Job with name '{job_data.name}' already exists")
    
    def get_job(self, job_id: int) -> Optional[Job]:
        """Get a job by ID."""
        return self.db.query(Job).filter(Job.id == job_id).first()
    
    def get_job_by_name(self, name: str) -> Optional[Job]:
        """Get a job by name."""
        return self.db.query(Job).filter(Job.name == name).first()
    
    def get_jobs(self, skip: int = 0, limit: int = 100, active_only: bool = False) -> List[Job]:
        """Get all jobs with pagination."""
        query = self.db.query(Job)
        
        if active_only:
            query = query.filter(Job.active == True)
            
        return query.order_by(Job.id).offset(skip).limit(limit).all()
    
    def update_job(self, job_id: int, job_data: JobUpdate) -> Optional[Job]:
        """Update an existing job."""
        job = self.get_job(job_id)
        if not job:
            return None
        
        update_data = job_data.dict(exclude_unset=True)
        
        for field, value in update_data.items():
            setattr(job, field, value)
        
        try:
            self.db.commit()
            self.db.refresh(job)
            return job
        except IntegrityError:
            self.db.rollback()
            raise ValueError(f"Job update failed due to constraint violation")
    
    def delete_job(self, job_id: int) -> bool:
        """Delete a job by ID."""
        job = self.get_job(job_id)
        if not job:
            return False
        
        self.db.delete(job)
        self.db.commit()
        return True
    
    def activate_job(self, job_id: int) -> Optional[Job]:
        """Activate a job."""
        return self.update_job(job_id, JobUpdate(active=True))
    
    def deactivate_job(self, job_id: int) -> Optional[Job]:
        """Deactivate a job."""
        return self.update_job(job_id, JobUpdate(active=False))