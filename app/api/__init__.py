# app/api/v1/__init__.py

from fastapi import APIRouter

from app.api.v1.jobs import router as jobs_router
from app.api.v1.health import router as health_router

# Create main API router
api_router = APIRouter(prefix="/api/v1")

# Include all sub-routers
api_router.include_router(
    jobs_router,
    prefix="",  # jobs router already has /jobs prefix
    tags=["jobs"]
)

api_router.include_router(
    health_router,
    prefix="",  # health router already has /health prefix
    tags=["health", "monitoring"]
)

# Additional routers can be added here as the API grows
# api_router.include_router(auth_router, prefix="/auth", tags=["authentication"])
# api_router.include_router(users_router, prefix="/users", tags=["users"])
# api_router.include_router(admin_router, prefix="/admin", tags=["admin"])

__all__ = ["api_router"]