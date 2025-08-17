# app/api/middleware.py

from fastapi import Request, Response, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp
import time
import uuid
import json
from typing import Callable, Optional, Dict, Any, Set
from datetime import datetime, timezone
import asyncio
from contextlib import asynccontextmanager
import redis.asyncio as aioredis

from app.core.config import get_settings
from app.utils.logger import get_logger
from app.core.exceptions import RateLimitExceededException

settings = get_settings()
logger = get_logger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add a unique request ID to each request.
    This helps with request tracing and debugging.
    """
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Generate or extract request ID
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        
        # Store request ID in request state for access in handlers
        request.state.request_id = request_id
        
        # Add request ID to logger context
        logger.bind(request_id=request_id)
        
        # Process request
        response = await call_next(request)
        
        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id
        
        return response


class TimingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to measure and log request processing time.
    Adds performance metrics to response headers.
    """
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start_time = time.time()
        
        # Process request
        response = await call_next(request)
        
        # Calculate processing time
        process_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Add timing headers
        response.headers["X-Process-Time-MS"] = str(round(process_time, 2))
        
        # Log slow requests
        if process_time > 1000:  # Log requests taking more than 1 second
            logger.warning(
                f"Slow request detected",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "process_time_ms": process_time,
                    "request_id": getattr(request.state, "request_id", "unknown")
                }
            )
        
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Redis-based distributed rate limiting middleware.
    Implements sliding window rate limiting.
    """
    
    def __init__(
        self,
        app: ASGIApp,
        calls: int = 100,
        period: int = 60,
        identifier: Optional[Callable[[Request], str]] = None,
        exclude_paths: Optional[Set[str]] = None
    ):
        super().__init__(app)
        self.calls = calls
        self.period = period
        self.identifier = identifier or self._default_identifier
        self.exclude_paths = exclude_paths or {"/health", "/metrics", "/docs", "/redoc", "/openapi.json"}
        self._redis: Optional[aioredis.Redis] = None
    
    async def _get_redis(self) -> aioredis.Redis:
        """Get or create Redis connection."""
        if not self._redis:
            self._redis = await aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
        return self._redis
    
    def _default_identifier(self, request: Request) -> str:
        """Default identifier using IP address and user ID if available."""
        # Try to get user ID from request state (set by auth middleware)
        user_id = getattr(request.state, "user_id", None)
        if user_id:
            return f"user:{user_id}"
        
        # Fall back to IP address
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return f"ip:{forwarded.split(',')[0].strip()}"
        
        client = request.client
        if client:
            return f"ip:{client.host}"
        
        return "ip:unknown"
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Skip rate limiting for excluded paths
        if request.url.path in self.exclude_paths:
            return await call_next(request)
        
        # Skip rate limiting for internal health checks
        if request.headers.get("X-Internal-Request") == "true":
            return await call_next(request)
        
        # Get identifier for this request
        identifier = self.identifier(request)
        key = f"rate_limit:{identifier}:{request.url.path}"
        
        try:
            redis = await self._get_redis()
            
            # Implement sliding window rate limiting
            now = time.time()
            window_start = now - self.period
            
            # Remove old entries
            await redis.zremrangebyscore(key, 0, window_start)
            
            # Count requests in current window
            request_count = await redis.zcard(key)
            
            if request_count >= self.calls:
                # Calculate retry after
                oldest_request = await redis.zrange(key, 0, 0, withscores=True)
                if oldest_request:
                    retry_after = int(self.period - (now - oldest_request[0][1]))
                else:
                    retry_after = self.period
                
                logger.warning(
                    f"Rate limit exceeded",
                    extra={
                        "identifier": identifier,
                        "path": request.url.path,
                        "request_count": request_count,
                        "limit": self.calls
                    }
                )
                
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "error": "Rate limit exceeded",
                        "message": f"Too many requests. Please try again in {retry_after} seconds.",
                        "retry_after": retry_after
                    },
                    headers={
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(self.calls),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(int(now + retry_after))
                    }
                )
            
            # Add current request to window
            await redis.zadd(key, {str(uuid.uuid4()): now})
            await redis.expire(key, self.period)
            
            # Process request
            response = await call_next(request)
            
            # Add rate limit headers
            remaining = self.calls - request_count - 1
            response.headers["X-RateLimit-Limit"] = str(self.calls)
            response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
            response.headers["X-RateLimit-Reset"] = str(int(now + self.period))
            
            return response
            
        except Exception as e:
            logger.error(f"Rate limiting error: {str(e)}", exc_info=True)
            # Allow request to proceed if rate limiting fails
            return await call_next(request)


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """
    Global error handling middleware.
    Catches unhandled exceptions and returns consistent error responses.
    """
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            return await call_next(request)
            
        except HTTPException as http_exc:
            # Re-raise HTTP exceptions as they already have proper handling
            raise http_exc
            
        except Exception as exc:
            # Log the error with full context
            request_id = getattr(request.state, "request_id", "unknown")
            logger.error(
                f"Unhandled exception",
                extra={
                    "request_id": request_id,
                    "path": request.url.path,
                    "method": request.method,
                    "error": str(exc)
                },
                exc_info=True
            )
            
            # Return generic error response
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "error": "Internal server error",
                    "message": "An unexpected error occurred. Please try again later.",
                    "request_id": request_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )


class CompressionMiddleware(BaseHTTPMiddleware):
    """
    Response compression middleware.
    Compresses responses using gzip for better performance.
    """
    
    def __init__(self, app: ASGIApp, minimum_size: int = 1024):
        super().__init__(app)
        self.minimum_size = minimum_size
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Check if client accepts gzip
        accept_encoding = request.headers.get("Accept-Encoding", "")
        if "gzip" not in accept_encoding:
            return await call_next(request)
        
        # Process request
        response = await call_next(request)
        
        # Only compress certain content types
        content_type = response.headers.get("Content-Type", "")
        compressible_types = ["application/json", "text/", "application/javascript"]
        
        if not any(ct in content_type for ct in compressible_types):
            return response
        
        # Check content length
        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) < self.minimum_size:
            return response
        
        # For streaming responses, we can't compress
        if hasattr(response, "body_iterator"):
            return response
        
        # Compress the response body
        import gzip
        
        body = b""
        async for chunk in response.body_iterator:
            body += chunk
        
        if len(body) < self.minimum_size:
            # Don't compress small responses
            return Response(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type
            )
        
        compressed_body = gzip.compress(body)
        
        # Only use compressed version if it's actually smaller
        if len(compressed_body) < len(body):
            headers = dict(response.headers)
            headers["Content-Encoding"] = "gzip"
            headers["Content-Length"] = str(len(compressed_body))
            headers["Vary"] = "Accept-Encoding"
            
            return Response(
                content=compressed_body,
                status_code=response.status_code,
                headers=headers,
                media_type=response.media_type
            )
        
        return Response(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type
        )


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Security headers middleware.
    Adds security-related headers to all responses.
    """
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        
        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        # Add CSP for API endpoints (not documentation)
        if not request.url.path.startswith("/docs") and not request.url.path.startswith("/redoc"):
            response.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none';"
        
        # Add HSTS for production
        if settings.ENVIRONMENT == "production":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Request/Response logging middleware.
    Logs detailed information about each request and response.
    """
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Get request details
        request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
        
        # Log request
        logger.info(
            f"Request started",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "query_params": dict(request.query_params),
                "client_host": request.client.host if request.client else None,
                "user_agent": request.headers.get("User-Agent")
            }
        )
        
        # Process request
        start_time = time.time()
        response = await call_next(request)
        duration = (time.time() - start_time) * 1000
        
        # Log response
        logger.info(
            f"Request completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round(duration, 2)
            }
        )
        
        return response


class CORSMiddleware(BaseHTTPMiddleware):
    """
    Custom CORS middleware with fine-grained control.
    """
    
    def __init__(
        self,
        app: ASGIApp,
        allow_origins: list = None,
        allow_methods: list = None,
        allow_headers: list = None,
        allow_credentials: bool = True,
        max_age: int = 86400
    ):
        super().__init__(app)
        self.allow_origins = allow_origins or settings.CORS_ORIGINS
        self.allow_methods = allow_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
        self.allow_headers = allow_headers or ["*"]
        self.allow_credentials = allow_credentials
        self.max_age = max_age
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Handle preflight requests
        if request.method == "OPTIONS":
            return self._preflight_response(request)
        
        # Process request
        response = await call_next(request)
        
        # Add CORS headers
        origin = request.headers.get("Origin")
        if origin:
            if self._is_allowed_origin(origin):
                response.headers["Access-Control-Allow-Origin"] = origin
                if self.allow_credentials:
                    response.headers["Access-Control-Allow-Credentials"] = "true"
                response.headers["Vary"] = "Origin"
        
        return response
    
    def _is_allowed_origin(self, origin: str) -> bool:
        if "*" in self.allow_origins:
            return True
        return origin in self.allow_origins
    
    def _preflight_response(self, request: Request) -> Response:
        headers = {
            "Access-Control-Allow-Methods": ", ".join(self.allow_methods),
            "Access-Control-Allow-Headers": ", ".join(self.allow_headers),
            "Access-Control-Max-Age": str(self.max_age)
        }
        
        origin = request.headers.get("Origin")
        if origin and self._is_allowed_origin(origin):
            headers["Access-Control-Allow-Origin"] = origin
            if self.allow_credentials:
                headers["Access-Control-Allow-Credentials"] = "true"
        
        return Response(status_code=200, headers=headers)


# Middleware registration function
def register_middleware(app):
    """Register all middleware in the correct order."""
    
    # Order matters! First middleware processes request first, last middleware processes response first
    
    # 1. Request ID (should be first to track all requests)
    app.add_middleware(RequestIDMiddleware)
    
    # 2. Security headers
    app.add_middleware(SecurityHeadersMiddleware)
    
    # 3. CORS (needs to be early for preflight requests)
    app.add_middleware(CORSMiddleware)
    
    # 4. Compression
    app.add_middleware(CompressionMiddleware)
    
    # 5. Error handling (catch all errors)
    app.add_middleware(ErrorHandlingMiddleware)
    
    # 6. Timing (measure actual processing time)
    app.add_middleware(TimingMiddleware)
    
    # 7. Rate limiting
    app.add_middleware(
        RateLimitMiddleware,
        calls=settings.RATE_LIMIT_CALLS,
        period=settings.RATE_LIMIT_PERIOD
    )
    
    # 8. Logging (log final request/response)
    app.add_middleware(LoggingMiddleware)
    
    logger.info("All middleware registered successfully")