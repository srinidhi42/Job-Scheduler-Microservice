from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Import models for easier access from elsewhere in the application
from .job import Job
from .schedule import Schedule, ScheduleType
from .execution import Execution, ExecutionStatus