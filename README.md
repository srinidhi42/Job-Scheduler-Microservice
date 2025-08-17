# Job-Scheduler-Microservice
A robust, scalable microservice for scheduling and executing jobs with customizable configurations, built following SOLID principles and best practices.

# Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Technology-Stack](#technology-stack)
- [Installation](#installation)
- [Configuration](#configuration)
- [API-Reference](#api-reference)
- [Deployment](#deployment)
- [License](#license)

## Overview
This Job Scheduler Microservice provides a reliable system for scheduling, managing, and executing recurring or one-time tasks. It supports various scheduling patterns (cron, interval, one-time), tracks execution history, and provides a RESTful API for job management.
Designed to handle significant scale: ~10,000 users globally, ~1,000 services, and ~6,000 API requests per minute.

## Features

- **Job Scheduling**: Schedule jobs using cron expressions, fixed intervals, or one-time execution  
- **Customizable Jobs**: Configure job parameters, retry policies, and timeout settings  
- **RESTful API**: Comprehensive API for managing jobs and schedules  
- **Execution History**: Complete execution history with status, output, and performance metrics  
- **Robust Error Handling**: Automatic retries, timeouts, and error logging  
- **Scalable Architecture**: Designed to handle thousands of concurrent jobs  
- **Monitoring & Metrics**: Prometheus metrics, Grafana dashboards, and health checks  
- **Database Integration**: Persistent storage of jobs, schedules, and execution records  
- **Containerization**: Docker and Docker Compose for easy deployment 

## Architecture

```mermaid
flowchart TB
    A[API Layer] --> B[Service Layer] --> C[Data Layer]
    A --> D[Schemas]
    B --> E[Scheduler]
    C --> F[Database]
    E --> G[Task Queue] --> H[Workers]
```


- **API Layer**: FastAPI endpoints for job management  
- **Service Layer**: Core business logic and job scheduling  
- **Data Layer**: SQLAlchemy models and database operations  
- **Task Queue**: Celery-based distributed task execution  
- **Workers**: Distributed job execution with retry logic 

## Technology Stack

- **Python 3.10+**: Core programming language  
- **FastAPI**: High-performance API framework  
- **SQLAlchemy**: ORM for database operations  
- **Alembic**: Database migrations  
- **Celery**: Distributed task queue  
- **Redis**: Message broker and caching  
- **PostgreSQL**: Primary database  
- **Prometheus/Grafana**: Metrics and monitoring  
- **Docker/Docker Compose**: Containerization and orchestration  
- **Pydantic**: Data validation and settings management  

## Installation

### Prerequisites

- **Python 3.10+**
- **PostgreSQL**
- **Redis**

## Local Development Setup

Clone the repository:
```
git clone https://github.com/srinidhi42/Job-Scheduler-Microservice.git
cd Job-Scheduler-Microservice
```


Create and activate a virtual environment:
```
python -m venv venv
source venv/bin/activate # On Windows: venv\Scripts\activate
```


Install dependencies:
```
pip install -r requirements.txt
```

Set up environment variables:
```
cp .env.example .env
# Edit .env with your configuration
```


Run database migrations:
```
alembic upgrade head
```


Start the API server:
```
uvicorn main:app --reload
```


Start Celery worker:
```
celery -A tasks.celery_app worker --loglevel=info
```

Start Celery beat:
```
celery -A tasks.celery_app beat --loglevel=info
```

Docker Setup

Build and start the services:
```
docker-compose -f docker/docker-compose.yml up -d
```

## Configuration
The service uses environment variables for configuration:



| Variable           | Description                    | Default                                                |
|--------------------|--------------------------------|--------------------------------------------------------|
| `DATABASE_URL`     | PostgreSQL connection string   | `postgresql://postgres:postgres@localhost:5432/scheduler` |
| `REDIS_HOST`       | Redis host                     | `localhost`                                            |
| `REDIS_PORT`       | Redis port                     | `6379`                                                 |
| `LOG_LEVEL`        | Logging level                  | `INFO`                                                 |
| `LOG_FORMAT`       | Log format (`json`/`text`)     | `json` in production, `text` otherwise                 |
| `METRICS_ENABLED`  | Enable Prometheus metrics      | `true`                                                 |
| `WORKER_CONCURRENCY` | Celery worker concurrency     | `4`                                                    |

## API Reference

Jobs API

| Endpoint                        | Method | Description            |
|----------------------------------|--------|------------------------|
| `/jobs`                          | GET    | List all jobs          |
| `/jobs/{job_id}`                 | GET    | Get job details        |
| `/jobs`                          | POST   | Create a new job       |
| `/jobs/{job_id}`                 | PUT    | Update a job           |
| `/jobs/{job_id}`                 | DELETE | Delete a job           |
| `/jobs/{job_id}/activate`        | POST   | Activate a job         |
| `/jobs/{job_id}/deactivate`      | POST   | Deactivate a job       |


Schedules API

| Endpoint                          | Method | Description                 |
|-----------------------------------|--------|-----------------------------|
| `/jobs/{job_id}/schedules`        | GET    | List schedules for a job    |
| `/schedules/{schedule_id}`        | GET    | Get schedule details        |
| `/jobs/{job_id}/schedules`        | POST   | Create a schedule           |
| `/schedules/{schedule_id}`        | PUT    | Update a schedule           |
| `/schedules/{schedule_id}`        | DELETE | Delete a schedule           |

Executions API

| Endpoint                             | Method | Description                   |
|--------------------------------------|--------|-------------------------------|
| `/jobs/{job_id}/executions`          | GET    | List executions for a job     |
| `/executions/{execution_id}`         | GET    | Get execution details         |
| `/jobs/{job_id}/execute`             | POST   | Execute a job immediately     |
| `/executions/{execution_id}/cancel` | POST   | Cancel an execution           |
| `/executions/{execution_id}/retry`  | POST   | Retry a failed execution      |

## Deployment
Docker Deployment
The recommended deployment method is using Docker Compose:
```
docker-compose -f docker/docker-compose.yml up -d
```

Kubernetes Deployment
For production deployments, Kubernetes manifests are provided in the k8s/ directory:
```
kubectl apply -f k8s/
```

## License
This project is licensed under the MIT License - see the LICENSE file for details.





