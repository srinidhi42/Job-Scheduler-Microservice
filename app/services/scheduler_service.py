import pytz
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from croniter import croniter
from models.job import Job
from models.schedule import Schedule, ScheduleType
from models.execution import Execution, ExecutionStatus
from schemas.schedule import (
    ScheduleCreate, CronScheduleCreate, IntervalScheduleCreate, 
    OneTimeScheduleCreate, ScheduleUpdate
)
from .job_service import JobService
from .executor_service import ExecutorService

class SchedulerService:
    """Service for managing job schedules and triggering executions."""
    
    def __init__(self, db: Session):
        self.db = db
        self.job_service = JobService(db)
    
    def create_schedule(self, schedule_data: ScheduleCreate) -> Schedule:
        """Create a new schedule based on its type."""
        # Verify the job exists
        job = self.job_service.get_job(schedule_data.job_id)
        if not job:
            raise ValueError(f"Job with ID {schedule_data.job_id} not found")
        
        # Convert to dict and prepare data based on schedule type
        schedule_dict = schedule_data.dict(exclude_unset=True)
        
        # Create schedule object
        schedule = Schedule(**schedule_dict)
        
        # Calculate next run time based on schedule type
        self._calculate_next_run(schedule)
        
        try:
            self.db.add(schedule)
            self.db.commit()
            self.db.refresh(schedule)
            return schedule
        except Exception as e:
            self.db.rollback()
            raise ValueError(f"Failed to create schedule: {str(e)}")
    
    def get_schedule(self, schedule_id: int) -> Optional[Schedule]:
        """Get a schedule by ID."""
        return self.db.query(Schedule).filter(Schedule.id == schedule_id).first()
    
    def get_schedules_for_job(self, job_id: int, active_only: bool = False) -> List[Schedule]:
        """Get all schedules for a specific job."""
        query = self.db.query(Schedule).filter(Schedule.job_id == job_id)
        
        if active_only:
            query = query.filter(Schedule.active == True)
            
        return query.all()
    
    def update_schedule(self, schedule_id: int, schedule_data: ScheduleUpdate) -> Optional[Schedule]:
        """Update an existing schedule."""
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return None
        
        # Apply updates
        update_data = schedule_data.dict(exclude_unset=True)
        
        for field, value in update_data.items():
            setattr(schedule, field, value)
        
        # Recalculate next run time if necessary fields changed
        needs_recalculation = any(field in update_data for field in [
            'cron_expression', 'interval_seconds', 'scheduled_time', 
            'timezone', 'start_date', 'end_date', 'active'
        ])
        
        if needs_recalculation:
            self._calculate_next_run(schedule)
        
        try:
            self.db.commit()
            self.db.refresh(schedule)
            return schedule
        except Exception as e:
            self.db.rollback()
            raise ValueError(f"Failed to update schedule: {str(e)}")
    
    def delete_schedule(self, schedule_id: int) -> bool:
        """Delete a schedule by ID."""
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return False
        
        self.db.delete(schedule)
        self.db.commit()
        return True
    
    def activate_schedule(self, schedule_id: int) -> Optional[Schedule]:
        """Activate a schedule."""
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return None
            
        schedule.active = True
        self._calculate_next_run(schedule)
        
        self.db.commit()
        self.db.refresh(schedule)
        return schedule
    
    def deactivate_schedule(self, schedule_id: int) -> Optional[Schedule]:
        """Deactivate a schedule."""
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return None
            
        schedule.active = False
        schedule.next_scheduled_time = None
        
        self.db.commit()
        self.db.refresh(schedule)
        return schedule
    
    def get_due_schedules(self, current_time: Optional[datetime] = None) -> List[Schedule]:
        """Get all schedules that are due to run."""
        if current_time is None:
            current_time = datetime.utcnow()
            
        return self.db.query(Schedule).filter(
            Schedule.active == True,
            Schedule.next_scheduled_time <= current_time
        ).all()
    
    def trigger_due_jobs(self, executor_service: ExecutorService) -> List[Execution]:
        """Check for due jobs and trigger their execution."""
        due_schedules = self.get_due_schedules()
        executions = []
        
        for schedule in due_schedules:
            # Create an execution record
            execution = self._create_execution_for_schedule(schedule)
            executions.append(execution)
            
            # Update the last scheduled time
            schedule.last_scheduled_time = schedule.next_scheduled_time
            
            # Calculate the next run time
            self._calculate_next_run(schedule)
            
            # Submit the job for execution
            executor_service.submit_execution(execution.id)
        
        self.db.commit()
        return executions
    
    def _create_execution_for_schedule(self, schedule: Schedule) -> Execution:
        """Create an execution record for a scheduled job."""
        execution = Execution(
            job_id=schedule.job_id,
            schedule_id=schedule.id,
            status=ExecutionStatus.PENDING,
            scheduled_time=schedule.next_scheduled_time
        )
        
        self.db.add(execution)
        self.db.flush()  # Generate ID without committing
        return execution
    
    def _calculate_next_run(self, schedule: Schedule) -> None:
        """Calculate the next run time for a schedule based on its type."""
        if not schedule.active:
            schedule.next_scheduled_time = None
            return
            
        current_time = datetime.utcnow()
        
        # Handle start_date: if schedule hasn't started yet, use start_date as base
        base_time = current_time
        if schedule.start_date and schedule.start_date > current_time:
            base_time = schedule.start_date
            
        # Check if schedule has expired
        if schedule.end_date and schedule.end_date <= current_time:
            schedule.active = False
            schedule.next_scheduled_time = None
            return
            
        # Calculate next run based on schedule type
        if schedule.type == ScheduleType.CRON and schedule.cron_expression:
            # Parse timezone
            tz = pytz.timezone(schedule.timezone) if schedule.timezone else pytz.UTC
            
            # Convert base_time to timezone
            base_time_tz = base_time.replace(tzinfo=pytz.UTC).astimezone(tz)
            
            # Create croniter instance
            cron = croniter(schedule.cron_expression, base_time_tz)
            
            # Get next run time and convert back to UTC
            next_run_tz = cron.get_next(datetime)
            next_run = next_run_tz.astimezone(pytz.UTC).replace(tzinfo=None)
            
            schedule.next_scheduled_time = next_run
            
        elif schedule.type == ScheduleType.INTERVAL and schedule.interval_seconds:
            # If there's a last scheduled time, use that as base
            if schedule.last_scheduled_time and schedule.last_scheduled_time > base_time:
                base_time = schedule.last_scheduled_time
                
            # Calculate next run by adding interval
            schedule.next_scheduled_time = base_time + timedelta(seconds=schedule.interval_seconds)
            
        elif schedule.type == ScheduleType.ONE_TIME and schedule.scheduled_time:
            # For one-time schedules, next run is just the scheduled time
            # If it's in the past, mark as inactive
            if schedule.scheduled_time <= current_time:
                if not schedule.last_scheduled_time:  # Only if it hasn't run yet
                    schedule.next_scheduled_time = schedule.scheduled_time
                else:
                    schedule.active = False
                    schedule.next_scheduled_time = None
            else:
                schedule.next_scheduled_time = schedule.scheduled_time
        else:
            # Invalid schedule configuration
            schedule.next_scheduled_time = None