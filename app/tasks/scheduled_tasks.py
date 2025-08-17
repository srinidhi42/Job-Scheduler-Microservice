import os
import time
import psutil
from datetime import datetime, timedelta
from celery import shared_task, group, chain
from celery.utils.log import get_task_logger
from sqlalchemy import and_, or_, func
from models.schedule import Schedule
from models.execution import Execution, ExecutionStatus
from models.job import Job
from .celery_app import get_db_session
from .job_tasks import execute_job, process_job_result

logger = get_task_logger(__name__)

@shared_task(name="tasks.scheduled_tasks.check_due_jobs")
def check_due_jobs():
    """Check for due jobs and trigger their execution."""
    db = get_db_session()
    triggered_jobs = 0
    
    try:
        # Get current time
        current_time = datetime.utcnow()
        
        # Find all active schedules that are due
        due_schedules = db.query(Schedule).filter(
            Schedule.active == True,
            Schedule.next_scheduled_time <= current_time
        ).all()
        
        # Process each due schedule
        for schedule in due_schedules:
            # Check if the job is active
            job = db.query(Job).filter(Job.id == schedule.job_id, Job.active == True).first()
            if not job:
                logger.warning(f"Job {schedule.job_id} for schedule {schedule.id} is not active or does not exist")
                continue
                
            # Create an execution record
            execution = Execution(
                job_id=schedule.job_id,
                schedule_id=schedule.id,
                status=ExecutionStatus.PENDING,
                scheduled_time=schedule.next_scheduled_time
            )
            
            db.add(execution)
            db.flush()  # Generate ID without committing
            
            # Update the last scheduled time
            schedule.last_scheduled_time = schedule.next_scheduled_time
            
            # Calculate the next run time based on schedule type
            _calculate_next_run(db, schedule, current_time)
            
            # Trigger the job execution asynchronously
            chain(
                execute_job.s(execution.id),
                process_job_result.s()
            ).apply_async()
            
            triggered_jobs += 1
        
        # Commit all changes
        db.commit()
        
        return {
            "status": "success",
            "triggered_jobs": triggered_jobs,
            "checked_at": current_time.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error checking due jobs: {str(e)}")
        db.rollback()
        return {"status": "error", "error": str(e)}
        
    finally:
        db.close()

@shared_task(name="tasks.scheduled_tasks.cleanup_old_executions")
def cleanup_old_executions(days_to_keep=30):
    """Clean up old execution records to prevent database bloat."""
    db = get_db_session()
    
    try:
        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        # Count executions to delete
        count = db.query(func.count(Execution.id)).filter(
            Execution.created_at < cutoff_date,
            Execution.status.in_([
                ExecutionStatus.SUCCESS.value,
                ExecutionStatus.FAILED.value,
                ExecutionStatus.CANCELLED.value,
                ExecutionStatus.TIMEOUT.value
            ])
        ).scalar()
        
        # Delete old executions
        deleted = db.query(Execution).filter(
            Execution.created_at < cutoff_date,
            Execution.status.in_([
                ExecutionStatus.SUCCESS.value,
                ExecutionStatus.FAILED.value,
                ExecutionStatus.CANCELLED.value,
                ExecutionStatus.TIMEOUT.value
            ])
        ).delete(synchronize_session=False)
        
        db.commit()
        
        return {
            "status": "success",
            "deleted_count": deleted,
            "cutoff_date": cutoff_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up old executions: {str(e)}")
        db.rollback()
        return {"status": "error", "error": str(e)}
        
    finally:
        db.close()

@shared_task(name="tasks.scheduled_tasks.health_check")
def health_check():
    """Check the health of the scheduler service and its dependencies."""
    db = get_db_session()
    health_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "service": "scheduler",
        "status": "healthy",
        "checks": {}
    }
    
    try:
        # Check database connectivity
        start_time = time.time()
        db.execute("SELECT 1")
        db_response_time = time.time() - start_time
        
        health_data["checks"]["database"] = {
            "status": "healthy",
            "response_time_ms": round(db_response_time * 1000, 2)
        }
        
        # Check system resources
        process = psutil.Process(os.getpid())
        
        # Memory usage
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        health_data["checks"]["memory"] = {
            "status": "healthy" if memory_percent < 80 else "warning",
            "usage_percent": round(memory_percent, 2),
            "rss_mb": round(memory_info.rss / (1024 * 1024), 2)
        }
        
        # CPU usage
        cpu_percent = process.cpu_percent(interval=0.5)
        
        health_data["checks"]["cpu"] = {
            "status": "healthy" if cpu_percent < 80 else "warning",
            "usage_percent": round(cpu_percent, 2)
        }
        
        # Check pending executions
        pending_count = db.query(func.count(Execution.id)).filter(
            Execution.status == ExecutionStatus.PENDING
        ).scalar()
        
        # Check running executions
        running_count = db.query(func.count(Execution.id)).filter(
            Execution.status == ExecutionStatus.RUNNING
        ).scalar()
        
        # Check executions older than 1 hour
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        stuck_count = db.query(func.count(Execution.id)).filter(
            Execution.status == ExecutionStatus.RUNNING,
            Execution.start_time < one_hour_ago
        ).scalar()
        
        health_data["checks"]["executions"] = {
            "status": "healthy" if stuck_count == 0 else "warning",
            "pending_count": pending_count,
            "running_count": running_count,
            "stuck_count": stuck_count
        }
        
        # Overall status
        if any(check["status"] != "healthy" for check in health_data["checks"].values()):
            health_data["status"] = "warning"
        
        return health_data
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        health_data["status"] = "unhealthy"
        health_data["error"] = str(e)
        return health_data
        
    finally:
        db.close()

def _calculate_next_run(db, schedule, current_time=None):
    """Calculate the next run time for a schedule based on its type."""
    from croniter import croniter
    import pytz
    
    if current_time is None:
        current_time = datetime.utcnow()
        
    # If schedule is not active, clear next run time
    if not schedule.active:
        schedule.next_scheduled_time = None
        return
        
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
    if schedule.type.value == "cron" and schedule.cron_expression:
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
        
    elif schedule.type.value == "interval" and schedule.interval_seconds:
        # If there's a last scheduled time, use that as base
        if schedule.last_scheduled_time and schedule.last_scheduled_time > base_time:
            base_time = schedule.last_scheduled_time
            
        # Calculate next run by adding interval
        schedule.next_scheduled_time = base_time + timedelta(seconds=schedule.interval_seconds)
        
    elif schedule.type.value == "one_time" and schedule.scheduled_time:
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