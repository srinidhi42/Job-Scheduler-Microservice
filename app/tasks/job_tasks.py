import time
import subprocess
import traceback
from datetime import datetime
from celery import shared_task, chain, group
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy.orm import Session
from models.execution import Execution, ExecutionStatus
from models.job import Job
from .celery_app import celery_app, get_db_session
import logging

logger = logging.getLogger(__name__)

@shared_task(bind=True, name="tasks.job_tasks.execute_job")
def execute_job(self, execution_id: int):
    """Execute a job based on its execution record."""
    db = get_db_session()
    
    try:
        # Get the execution record
        execution = db.query(Execution).filter(Execution.id == execution_id).first()
        if not execution:
            logger.error(f"Execution {execution_id} not found")
            return {"status": "error", "error": "Execution not found"}
        
        # Get the job
        job = db.query(Job).filter(Job.id == execution.job_id).first()
        if not job:
            logger.error(f"Job {execution.job_id} not found for execution {execution_id}")
            _update_execution_status(
                db, execution_id, 
                status=ExecutionStatus.FAILED, 
                error="Job not found"
            )
            return {"status": "error", "error": "Job not found"}
        
        # Mark as running
        start_time = datetime.utcnow()
        _update_execution_status(
            db, execution_id,
            status=ExecutionStatus.RUNNING,
            start_time=start_time
        )
        
        # Execute the command
        logger.info(f"Executing job {job.id} (execution {execution_id}): {job.command}")
        
        try:
            # Execute the command with timeout
            process = subprocess.Popen(
                job.command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for process to complete with timeout (job.timeout_seconds)
            stdout, stderr = process.communicate(timeout=job.timeout_seconds)
            exit_code = process.returncode
            
            # Determine execution status
            status = ExecutionStatus.SUCCESS if exit_code == 0 else ExecutionStatus.FAILED
            
            return {
                "status": "complete",
                "execution_id": execution_id,
                "exit_code": exit_code,
                "output": stdout,
                "error": stderr,
                "execution_status": status.value,
                "start_time": start_time.isoformat(),
                "end_time": datetime.utcnow().isoformat()
            }
            
        except subprocess.TimeoutExpired:
            # Kill the process if it times out
            process.kill()
            stdout, stderr = process.communicate()
            
            error_message = f"{stderr}\nExecution timed out after {job.timeout_seconds} seconds"
            
            return {
                "status": "timeout",
                "execution_id": execution_id,
                "output": stdout,
                "error": error_message,
                "execution_status": ExecutionStatus.TIMEOUT.value,
                "start_time": start_time.isoformat(),
                "end_time": datetime.utcnow().isoformat()
            }
            
    except SoftTimeLimitExceeded:
        logger.error(f"Task soft time limit exceeded for execution {execution_id}")
        return {
            "status": "celery_timeout",
            "execution_id": execution_id,
            "error": "Celery task timed out",
            "execution_status": ExecutionStatus.TIMEOUT.value
        }
        
    except Exception as e:
        logger.error(f"Error executing job {execution_id}: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            "status": "error",
            "execution_id": execution_id,
            "error": f"Error executing job: {str(e)}",
            "execution_status": ExecutionStatus.FAILED.value
        }
        
    finally:
        db.close()

@shared_task(bind=True, name="tasks.job_tasks.process_job_result")
def process_job_result(self, result):
    """Process the result of a job execution."""
    db = get_db_session()
    
    try:
        execution_id = result.get("execution_id")
        if not execution_id:
            logger.error(f"No execution_id in result: {result}")
            return
        
        # Get the execution status
        execution_status = result.get("execution_status", ExecutionStatus.FAILED.value)
        status = ExecutionStatus(execution_status)
        
        # Parse start and end times
        start_time = datetime.fromisoformat(result.get("start_time")) if result.get("start_time") else None
        end_time = datetime.fromisoformat(result.get("end_time")) if result.get("end_time") else datetime.utcnow()
        
        # Update the execution record
        _update_execution_status(
            db, execution_id,
            status=status,
            start_time=start_time,
            end_time=end_time,
            exit_code=result.get("exit_code"),
            output=result.get("output"),
            error=result.get("error")
        )
        
        # Check if this job needs to be retried
        if status in [ExecutionStatus.FAILED, ExecutionStatus.TIMEOUT]:
            execution = db.query(Execution).filter(Execution.id == execution_id).first()
            if execution:
                job = db.query(Job).filter(Job.id == execution.job_id).first()
                if job and job.max_retries > 0 and execution.retry_count < job.max_retries:
                    # Schedule a retry
                    handle_job_retry.delay(execution_id)
        
        return {"status": "success", "execution_id": execution_id}
        
    except Exception as e:
        logger.error(f"Error processing job result: {str(e)}")
        logger.error(traceback.format_exc())
        return {"status": "error", "error": str(e)}
        
    finally:
        db.close()

@shared_task(bind=True, name="tasks.job_tasks.handle_job_retry")
def handle_job_retry(self, execution_id: int):
    """Handle retrying a failed job."""
    db = get_db_session()
    
    try:
        # Get the original execution
        execution = db.query(Execution).filter(Execution.id == execution_id).first()
        if not execution:
            logger.error(f"Execution {execution_id} not found for retry")
            return {"status": "error", "error": "Execution not found"}
        
        # Get the job
        job = db.query(Job).filter(Job.id == execution.job_id).first()
        if not job:
            logger.error(f"Job {execution.job_id} not found for execution retry {execution_id}")
            return {"status": "error", "error": "Job not found"}
        
        # Check if we can retry
        if execution.retry_count >= job.max_retries:
            logger.info(f"Maximum retries reached for execution {execution_id}")
            return {"status": "max_retries", "execution_id": execution_id}
        
        # Create a new execution as a retry
        new_execution = Execution(
            job_id=execution.job_id,
            schedule_id=execution.schedule_id,
            status=ExecutionStatus.PENDING,
            scheduled_time=datetime.utcnow(),
            retry_count=execution.retry_count + 1,
            retry_of=execution.id if execution.retry_of is None else execution.retry_of,
            metadata=execution.metadata
        )
        
        db.add(new_execution)
        db.commit()
        db.refresh(new_execution)
        
        # Submit the new execution
        chain(
            execute_job.s(new_execution.id),
            process_job_result.s()
        ).apply_async()
        
        return {"status": "retry_scheduled", "new_execution_id": new_execution.id}
        
    except Exception as e:
        logger.error(f"Error handling job retry: {str(e)}")
        logger.error(traceback.format_exc())
        db.rollback()
        return {"status": "error", "error": str(e)}
        
    finally:
        db.close()

def _update_execution_status(
    db: Session,
    execution_id: int,
    status: ExecutionStatus = None,
    start_time: datetime = None,
    end_time: datetime = None,
    exit_code: int = None,
    output: str = None,
    error: str = None
) -> bool:
    """Update the status and details of an execution."""
    try:
        execution = db.query(Execution).filter(Execution.id == execution_id).first()
        if not execution:
            logger.error(f"Execution {execution_id} not found for status update")
            return False
            
        if status is not None:
            execution.status = status
        
        if start_time is not None:
            execution.start_time = start_time
        
        if end_time is not None:
            execution.end_time = end_time
            
        if exit_code is not None:
            execution.exit_code = exit_code
            
        if output is not None:
            # Truncate if too long
            execution.output = output[:100000] if len(output) > 100000 else output
            
        if error is not None:
            # Truncate if too long
            execution.error = error[:100000] if len(error) > 100000 else error
        
        db.commit()
        return True
        
    except Exception as e:
        logger.error(f"Error updating execution {execution_id} status: {str(e)}")
        db.rollback()
        return False