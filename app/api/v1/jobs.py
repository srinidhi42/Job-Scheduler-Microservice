from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID
import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, Path, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.core.dependencies import (
    get_db,
    get_current_user,
    get_job_service,
    get_cache_service,
)
from app.schemas.job import (
    JobCreate,
    JobUpdate,
    JobResponse,
    JobListResponse,
    JobExecutionResponse,
    JobScheduleCreate,
    JobFilterParams,
)
from app.schemas.common import (
    PaginationParams,
    SuccessResponse,
    ErrorResponse,
)
from app.services.job_service import JobService
from app.services.cache_service import CacheService
from app.core.exceptions import (
    JobNotFoundException,
    InvalidScheduleException,
    JobExecutionException,
)
from app.models.user import User
from app.core.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
    responses={
        404: {"model": ErrorResponse, "description": "Job not found"},
        400: {"model": ErrorResponse, "description": "Bad request"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)


@router.get(
    "",
    response_model=JobListResponse,
    summary="List all jobs",
    description="Get a paginated list of jobs with optional filtering",
    response_description="List of jobs with pagination metadata",
)
async def list_jobs(
    # Pagination parameters
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    
    # Filter parameters
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    created_by: Optional[str] = Query(None, description="Filter by creator"),
    search: Optional[str] = Query(None, description="Search in job name and description"),
    
    # Sorting parameters
    sort_by: str = Query("created_at", regex="^(created_at|updated_at|next_run_at|name)$"),
    sort_order: str = Query("desc", regex="^(asc|desc)$"),
    
    # Dependencies
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> JobListResponse:
    """
    Retrieve a paginated list of jobs.
    
    This endpoint supports:
    - Pagination with configurable page size
    - Filtering by status, type, and creator
    - Full-text search in name and description
    - Sorting by multiple fields
    - Caching for improved performance
    """
    try:
        # Create filter params
        filters = JobFilterParams(
            is_active=is_active,
            job_type=job_type,
            created_by=created_by,
            search=search,
        )
        
        # Create pagination params
        pagination = PaginationParams(
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        
        # Generate cache key for this specific query
        cache_key = f"jobs:list:{filters.cache_key()}:{pagination.cache_key()}"
        
        # Try to get from cache first
        cached_result = await cache_service.get(cache_key)
        if cached_result:
            logger.info(f"Cache hit for job list: {cache_key}")
            return JobListResponse(**cached_result)
        
        # Get jobs from service (parallel execution for better performance)
        jobs_task = job_service.list_jobs(filters, pagination)
        count_task = job_service.count_jobs(filters)
        
        jobs, total_count = await asyncio.gather(jobs_task, count_task)
        
        # Calculate pagination metadata
        total_pages = (total_count + limit - 1) // limit
        has_next = page < total_pages
        has_prev = page > 1
        
        # Create response
        response = JobListResponse(
            items=[JobResponse.from_orm(job) for job in jobs],
            total=total_count,
            page=page,
            limit=limit,
            pages=total_pages,
            has_next=has_next,
            has_prev=has_prev,
        )
        
        # Cache the result (5 minutes TTL)
        await cache_service.set(cache_key, response.dict(), ttl=300)
        
        logger.info(f"Retrieved {len(jobs)} jobs for user {current_user.id}")
        return response
        
    except Exception as e:
        logger.error(f"Error listing jobs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve jobs",
        )


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job details",
    description="Get detailed information about a specific job",
)
async def get_job(
    job_id: UUID = Path(..., description="Job ID"),
    include_executions: bool = Query(False, description="Include execution history"),
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> JobResponse:
    """
    Retrieve detailed information about a specific job.
    
    Optionally includes execution history for the job.
    """
    try:
        # Try cache first
        cache_key = f"job:{job_id}:executions:{include_executions}"
        cached_job = await cache_service.get(cache_key)
        if cached_job:
            return JobResponse(**cached_job)
        
        # Get job from service
        job = await job_service.get_job(job_id, include_executions)
        
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found",
            )
        
        # Check access permissions
        if not await job_service.can_access_job(job, current_user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to access this job",
            )
        
        response = JobResponse.from_orm(job)
        
        # Cache the result
        await cache_service.set(cache_key, response.dict(), ttl=300)
        
        return response
        
    except JobNotFoundException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve job",
        )


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new job",
    description="Create a new scheduled job",
)
async def create_job(
    job_data: JobCreate,
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> JobResponse:
    """
    Create a new scheduled job.
    
    The job will be scheduled according to the provided schedule configuration.
    Supports cron expressions, intervals, and one-time executions.
    """
    try:
        # Validate job type
        if not await job_service.is_valid_job_type(job_data.job_type):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid job type: {job_data.job_type}",
            )
        
        # Validate schedule
        if job_data.schedule:
            try:
                await job_service.validate_schedule(job_data.schedule)
            except InvalidScheduleException as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=str(e),
                )
        
        # Create job with user context
        job = await job_service.create_job(
            job_data=job_data,
            created_by=current_user.id,
        )
        
        # Invalidate cache for job lists
        await cache_service.delete_pattern("jobs:list:*")
        
        logger.info(f"Job {job.id} created by user {current_user.id}")
        
        return JobResponse.from_orm(job)
        
    except IntegrityError as e:
        logger.error(f"Database integrity error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A job with this name already exists",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create job",
        )


@router.put(
    "/{job_id}",
    response_model=JobResponse,
    summary="Update a job",
    description="Update an existing job's configuration",
)
async def update_job(
    job_id: UUID = Path(..., description="Job ID"),
    job_update: JobUpdate = ...,
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> JobResponse:
    """
    Update an existing job's configuration.
    
    Only the job owner or administrators can update a job.
    """
    try:
        # Get existing job
        job = await job_service.get_job(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found",
            )
        
        # Check permissions
        if not await job_service.can_modify_job(job, current_user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to update this job",
            )
        
        # Update job
        updated_job = await job_service.update_job(job_id, job_update)
        
        # Invalidate cache
        await cache_service.delete(f"job:{job_id}:*")
        await cache_service.delete_pattern("jobs:list:*")
        
        logger.info(f"Job {job_id} updated by user {current_user.id}")
        
        return JobResponse.from_orm(updated_job)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update job",
        )


@router.delete(
    "/{job_id}",
    response_model=SuccessResponse,
    summary="Delete a job",
    description="Delete a job and all its associated data",
)
async def delete_job(
    job_id: UUID = Path(..., description="Job ID"),
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> SuccessResponse:
    """
    Delete a job and all its associated data.
    
    This will also cancel any pending executions.
    """
    try:
        # Get existing job
        job = await job_service.get_job(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found",
            )
        
        # Check permissions
        if not await job_service.can_modify_job(job, current_user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to delete this job",
            )
        
        # Delete job
        await job_service.delete_job(job_id)
        
        # Invalidate cache
        await cache_service.delete(f"job:{job_id}:*")
        await cache_service.delete_pattern("jobs:list:*")
        
        logger.info(f"Job {job_id} deleted by user {current_user.id}")
        
        return SuccessResponse(
            success=True,
            message=f"Job {job_id} successfully deleted",
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete job",
        )


@router.post(
    "/{job_id}/execute",
    response_model=JobExecutionResponse,
    summary="Execute a job immediately",
    description="Trigger immediate execution of a job",
)
async def execute_job(
    job_id: UUID = Path(..., description="Job ID"),
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
) -> JobExecutionResponse:
    """
    Trigger immediate execution of a job.
    
    This bypasses the schedule and executes the job immediately.
    """
    try:
        # Get job
        job = await job_service.get_job(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found",
            )
        
        # Check permissions
        if not await job_service.can_execute_job(job, current_user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to execute this job",
            )
        
        # Execute job
        execution = await job_service.execute_job_now(job_id, triggered_by=current_user.id)
        
        logger.info(f"Job {job_id} executed manually by user {current_user.id}")
        
        return JobExecutionResponse.from_orm(execution)
        
    except JobExecutionException as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute job: {str(e)}",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to execute job",
        )


@router.get(
    "/{job_id}/executions",
    response_model=List[JobExecutionResponse],
    summary="Get job execution history",
    description="Get the execution history for a specific job",
)
async def get_job_executions(
    job_id: UUID = Path(..., description="Job ID"),
    limit: int = Query(20, ge=1, le=100, description="Number of executions to retrieve"),
    status: Optional[str] = Query(None, regex="^(pending|running|completed|failed)$"),
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
) -> List[JobExecutionResponse]:
    """
    Get the execution history for a specific job.
    
    Returns the most recent executions, optionally filtered by status.
    """
    try:
        # Get job
        job = await job_service.get_job(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found",
            )
        
        # Check permissions
        if not await job_service.can_access_job(job, current_user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to access this job",
            )
        
        # Get executions
        executions = await job_service.get_job_executions(
            job_id=job_id,
            limit=limit,
            status=status,
        )
        
        return [JobExecutionResponse.from_orm(exec) for exec in executions]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving executions for job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve job executions",
        )


@router.patch(
    "/{job_id}/toggle",
    response_model=JobResponse,
    summary="Toggle job active status",
    description="Enable or disable a job",
)
async def toggle_job_status(
    job_id: UUID = Path(..., description="Job ID"),
    current_user: User = Depends(get_current_user),
    job_service: JobService = Depends(get_job_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> JobResponse:
    """
    Toggle a job's active status.
    
    If the job is active, it will be deactivated and vice versa.
    """
    try:
        # Get job
        job = await job_service.get_job(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found",
            )
        
        # Check permissions
        if not await job_service.can_modify_job(job, current_user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to modify this job",
            )
        
        # Toggle status
        updated_job = await job_service.toggle_job_status(job_id)
        
        # Invalidate cache
        await cache_service.delete(f"job:{job_id}:*")
        await cache_service.delete_pattern("jobs:list:*")
        
        action = "activated" if updated_job.is_active else "deactivated"
        logger.info(f"Job {job_id} {action} by user {current_user.id}")
        
        return JobResponse.from_orm(updated_job)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling job {job_id} status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to toggle job status",
        )