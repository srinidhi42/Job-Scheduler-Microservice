import os
import signal
import subprocess
import threading
import time
import json
from typing import List, Optional, Dict, Any, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from models.job import Job
from models.execution import Execution, ExecutionStatus
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

class ExecutorService:
    """Service for executing jobs and managing their lifecycle."""
    
    def __init__(self, db: Session, max_workers: int = 10):
        self.db = db
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.running_executions = {}  # Map of execution_id to process
    
    def submit_execution(self, execution_id: int) -> bool:
        """Submit an execution to be run asynchronously."""
        # Get the execution record
        execution = self.get_execution(execution_id)
        if not execution:
            logger.error(f"Execution {execution_id} not found")
            return False
            
        # Get the job
        job = self.get_job(execution.job_id)
        if not job:
            logger.error(f"Job {execution.job_id} not found for execution {execution_id}")
            self._update_execution_status(
                execution_id, 
                ExecutionStatus.FAILED, 
                error="Job not found"
            )
            return False
            
        # Submit to thread pool
        self.thread_pool.submit(self._execute_job, execution_id, job.command, job.timeout_seconds)
        return True
    
    def get_execution(self, execution_id: int) -> Optional[Execution]:
        """Get an execution by ID."""
        return self.db.query(Execution).filter(Execution.id == execution_id).first()
    
    def get_job(self, job_id: int) -> Optional[Job]:
        """Get a job by ID."""
        return self.db.query(Job).filter(Job.id == job_id).first()
    
    def get_executions_for_job(
        self, 
        job_id: int, 
        status: Optional[ExecutionStatus] = None,
        limit: int = 100
    ) -> List[Execution]:
        """Get executions for a specific job, optionally filtered by status."""
        query = self.db.query(Execution).filter(Execution.job_id == job_id)
        
        if status:
            query = query.filter(Execution.status == status)
            
        return query.order_by(Execution.id.desc()).limit(limit).all()
    
    def cancel_execution(self, execution_id: int) -> bool:
        """Cancel a running execution."""
        execution = self.get_execution(execution_id)
        if not execution:
            return False
            
        # Can only cancel pending or running executions
        if execution.status not in [ExecutionStatus.PENDING, ExecutionStatus.RUNNING]:
            return False
            
        # If the execution is running, terminate the process
        if execution_id in self.running_executions:
            process = self.running_executions[execution_id]
            try:
                # Try to terminate gracefully first
                process.terminate()
                
                # Wait a bit for it to exit
                for _ in range(5):
                    if process.poll() is not None:
                        break
                    time.sleep(0.1)
                
                # If still running, kill it
                if process.poll() is None:
                    process.kill()
                    
                del self.running_executions[execution_id]
            except Exception as e:
                logger.error(f"Error terminating execution {execution_id}: {str(e)}")
        
        # Update execution status
        self._update_execution_status(
            execution_id,
            ExecutionStatus.CANCELLED,
            error="Execution cancelled by user"
        )
        
        return True
    
    def retry_execution(self, execution_id: int) -> Optional[int]:
        """Retry a failed execution."""
        execution = self.get_execution(execution_id)
        if not execution:
            return None
            
        # Can only retry failed, cancelled, or timed out executions
        if execution.status not in [ExecutionStatus.FAILED, ExecutionStatus.CANCELLED, ExecutionStatus.TIMEOUT]:
            return None
            
        # Get the job to check max retries
        job = self.get_job(execution.job_id)
        if not job:
            return None
            
        # Check if max retries has been reached
        if execution.retry_count >= job.max_retries:
            return None
            
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
        
        try:
            self.db.add(new_execution)
            self.db.commit()
            self.db.refresh(new_execution)
            
            # Submit the new execution
            self.submit_execution(new_execution.id)
            
            return new_execution.id
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error creating retry execution: {str(e)}")
            return None
    
    def _execute_job(self, execution_id: int, command: str, timeout_seconds: int) -> None:
        """Execute a job command in a subprocess."""
        # Update status to running
        self._update_execution_status(execution_id, ExecutionStatus.RUNNING)
        
        start_time = datetime.utcnow()
        output = ""
        error = ""
        exit_code = None
        
        try:
            # Create process
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Register as running
            self.running_executions[execution_id] = process
            
            try:
                # Wait for process to complete with timeout
                stdout, stderr = process.communicate(timeout=timeout_seconds)
                exit_code = process.returncode
                output = stdout
                error = stderr
                
                # Determine final status
                status = ExecutionStatus.SUCCESS if exit_code == 0 else ExecutionStatus.FAILED
                
            except subprocess.TimeoutExpired:
                # Kill the process if it times out
                process.kill()
                stdout, stderr = process.communicate()
                output = stdout
                error = f"{stderr}\nExecution timed out after {timeout_seconds} seconds"
                status = ExecutionStatus.TIMEOUT
                exit_code = -1
                
        except Exception as e:
            # Handle any other errors
            status = ExecutionStatus.FAILED
            error = f"Error executing job: {str(e)}"
            exit_code = -1
            
        finally:
            # Remove from running executions
            if execution_id in self.running_executions:
                del self.running_executions[execution_id]
            
            end_time = datetime.utcnow()
            
            # Update execution record
            self._update_execution_status(
                execution_id,
                status,
                start_time=start_time,
                end_time=end_time,
                exit_code=exit_code,
                output=output,
                error=error
            )
    
    def _update_execution_status(
        self,
        execution_id: int,
        status: ExecutionStatus,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exit_code: Optional[int] = None,
        output: Optional[str] = None,
        error: Optional[str] = None
    ) -> bool:
        """Update the status and details of an execution."""
        try:
            # Create a new session for thread safety
            session = self.db.object_session(self.db.query(Execution).first())
            
            execution = session.query(Execution).filter(Execution.id == execution_id).first()
            if not execution:
                logger.error(f"Execution {execution_id} not found for status update")
                return False
                
            execution.status = status
            
            if start_time:
                execution.start_time = start_time
            
            if end_time:
                execution.end_time = end_time
                
            if exit_code is not None:
                execution.exit_code = exit_code
                
            if output is not None:
                # Truncate if too long
                execution.output = output[:100000] if len(output) > 100000 else output
                
            if error is not None:
                # Truncate if too long
                execution.error = error[:100000] if len(error) > 100000 else error
            
            session.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error updating execution {execution_id} status: {str(e)}")
            return False