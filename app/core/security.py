"""
Security module for the Job Scheduler Microservice.

Provides authentication, authorization, rate limiting, and security utilities.
"""

import jwt
import bcrypt
import secrets
import hashlib
import time
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from functools import wraps
from collections import defaultdict
import redis
import logging

from .exceptions import (
    AuthenticationError,
    AuthorizationError, 
    RateLimitExceededError,
    SecurityError
)

logger = logging.getLogger(__name__)


@dataclass
class User:
    """User model for authentication and authorization."""
    id: str
    username: str
    email: str
    password_hash: str
    roles: List[str] = field(default_factory=list)
    permissions: Set[str] = field(default_factory=set)
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    failed_login_attempts: int = 0
    locked_until: Optional[datetime] = None
    
    def has_permission(self, permission: str) -> bool:
        """Check if user has specific permission."""
        return permission in self.permissions
    
    def has_role(self, role: str) -> bool:
        """Check if user has specific role."""
        return role in self.roles
    
    def is_locked(self) -> bool:
        """Check if account is temporarily locked."""
        if self.locked_until is None:
            return False
        return datetime.utcnow() < self.locked_until


@dataclass
class JWTToken:
    """JWT token model."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 3600  # 1 hour default
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in
        }


class RateLimiter:
    """Redis-based rate limiter with sliding window."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.default_limits = {
            "api_calls": (100, 60),      # 100 calls per minute
            "job_creation": (10, 60),     # 10 job creations per minute
            "job_execution": (50, 300),   # 50 executions per 5 minutes
        }
    
    def is_allowed(
        self, 
        key: str, 
        limit: int, 
        window_seconds: int,
        identifier: Optional[str] = None
    ) -> Tuple[bool, int, int]:
        """
        Check if request is within rate limit using sliding window.
        
        Returns:
            Tuple of (is_allowed, current_count, remaining_count)
        """
        full_key = f"rate_limit:{key}"
        if identifier:
            full_key += f":{identifier}"
            
        current_time = time.time()
        window_start = current_time - window_seconds
        
        # Remove expired entries
        self.redis.zremrangebyscore(full_key, 0, window_start)
        
        # Count current requests in window
        current_count = self.redis.zcard(full_key)
        
        if current_count >= limit:
            return False, current_count, 0
        
        # Add current request
        self.redis.zadd(full_key, {str(current_time): current_time})
        self.redis.expire(full_key, window_seconds)
        
        remaining = limit - current_count - 1
        return True, current_count + 1, remaining
    
    def check_limit(
        self, 
        user_id: str, 
        action: str,
        custom_limit: Optional[Tuple[int, int]] = None
    ) -> None:
        """Check rate limit and raise exception if exceeded."""
        if custom_limit:
            limit, window = custom_limit
        else:
            limit, window = self.default_limits.get(action, (100, 60))
        
        allowed, current, remaining = self.is_allowed(
            action, limit, window, user_id
        )
        
        if not allowed:
            raise RateLimitExceededError(
                user_id=user_id,
                limit=limit,
                window_seconds=window,
                current_count=current
            )


class SecurityManager:
    """Main security manager handling authentication, authorization, and security policies."""
    
    def __init__(
        self, 
        secret_key: str,
        redis_client: redis.Redis,
        algorithm: str = "HS256",
        access_token_expire_minutes: int = 60,
        refresh_token_expire_days: int = 30
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.access_token_expire_minutes = access_token_expire_minutes
        self.refresh_token_expire_days = refresh_token_expire_days
        self.rate_limiter = RateLimiter(redis_client)
        self.redis = redis_client
        
        # In-memory user store (in production, use database)
        self.users: Dict[str, User] = {}
        self.user_sessions: Dict[str, Set[str]] = defaultdict(set)
        
        # Security policies
        self.max_failed_attempts = 5
        self.lockout_duration_minutes = 15
        self.password_min_length = 8
        
        # Role-based permissions
        self.role_permissions = {
            "admin": {
                "job:create", "job:read", "job:update", "job:delete",
                "job:execute", "job:cancel", "system:admin", "user:manage"
            },
            "operator": {
                "job:create", "job:read", "job:update", "job:execute", 
                "job:cancel"
            },
            "viewer": {"job:read"},
            "service": {
                "job:create", "job:read", "job:update", "job:execute"
            }
        }
    
    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt."""
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    
    def verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash."""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except Exception as e:
            logger.error(f"Password verification error: {e}")
            return False
    
    def validate_password_strength(self, password: str) -> List[str]:
        """Validate password meets security requirements."""
        errors = []
        
        if len(password) < self.password_min_length:
            errors.append(f"Password must be at least {self.password_min_length} characters long")
        
        if not any(c.isupper() for c in password):
            errors.append("Password must contain at least one uppercase letter")
        
        if not any(c.islower() for c in password):
            errors.append("Password must contain at least one lowercase letter")
        
        if not any(c.isdigit() for c in password):
            errors.append("Password must contain at least one digit")
        
        if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            errors.append("Password must contain at least one special character")
        
        return errors
    
    def create_user(
        self, 
        username: str, 
        email: str, 
        password: str,
        roles: Optional[List[str]] = None
    ) -> User:
        """Create a new user with password validation."""
        # Validate password
        password_errors = self.validate_password_strength(password)
        if password_errors:
            raise SecurityError(f"Password validation failed: {', '.join(password_errors)}")
        
        # Check if user already exists
        if any(u.username == username or u.email == email for u in self.users.values()):
            raise SecurityError("User with this username or email already exists")
        
        # Create user
        user_id = self._generate_user_id()
        roles = roles or ["viewer"]
        
        # Calculate permissions from roles
        permissions = set()
        for role in roles:
            if role in self.role_permissions:
                permissions.update(self.role_permissions[role])
        
        user = User(
            id=user_id,
            username=username,
            email=email,
            password_hash=self.hash_password(password),
            roles=roles,
            permissions=permissions
        )
        
        self.users[user_id] = user
        logger.info(f"Created user: {username} with roles: {roles}")
        return user
    
    def authenticate_user(self, username: str, password: str) -> User:
        """Authenticate user with username/password."""
        user = self._find_user_by_username(username)
        if not user:
            raise AuthenticationError("Invalid username or password")
        
        # Check if account is locked
        if user.is_locked():
            raise AuthenticationError(
                f"Account locked until {user.locked_until.isoformat()}"
            )
        
        # Check if account is active
        if not user.is_active:
            raise AuthenticationError("Account is disabled")
        
        # Verify password
        if not self.verify_password(password, user.password_hash):
            # Increment failed attempts
            user.failed_login_attempts += 1
            
            # Lock account if too many failures
            if user.failed_login_attempts >= self.max_failed_attempts:
                user.locked_until = datetime.utcnow() + timedelta(
                    minutes=self.lockout_duration_minutes
                )
                logger.warning(f"Account locked for user: {username}")
            
            raise AuthenticationError("Invalid username or password")
        
        # Reset failed attempts on successful login
        user.failed_login_attempts = 0
        user.locked_until = None
        user.last_login = datetime.utcnow()
        
        logger.info(f"User authenticated: {username}")
        return user
    
    def generate_tokens(self, user: User) -> JWTToken:
        """Generate JWT access and refresh tokens."""
        now = datetime.utcnow()
        
        # Access token payload
        access_payload = {
            "sub": user.id,
            "username": user.username,
            "roles": user.roles,
            "permissions": list(user.permissions),
            "iat": now,
            "exp": now + timedelta(minutes=self.access_token_expire_minutes),
            "type": "access"
        }
        
        # Refresh token payload
        refresh_payload = {
            "sub": user.id,
            "iat": now,
            "exp": now + timedelta(days=self.refresh_token_expire_days),
            "type": "refresh"
        }
        
        access_token = jwt.encode(access_payload, self.secret_key, algorithm=self.algorithm)
        refresh_token = jwt.encode(refresh_payload, self.secret_key, algorithm=self.algorithm)
        
        # Store session
        self.user_sessions[user.id].add(access_token)
        
        return JWTToken(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=self.access_token_expire_minutes * 60
        )
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check if token is in active sessions
            user_id = payload.get("sub")
            if user_id and token not in self.user_sessions.get(user_id, set()):
                raise AuthenticationError("Token has been revoked")
            
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token has expired")
        except jwt.InvalidTokenError as e:
            raise AuthenticationError(f"Invalid token: {str(e)}")
    
    def refresh_token(self, refresh_token: str) -> JWTToken:
        """Generate new access token from refresh token."""
        payload = self.verify_token(refresh_token)
        
        if payload.get("type") != "refresh":
            raise AuthenticationError("Invalid refresh token")
        
        user_id = payload.get("sub")
        user = self.users.get(user_id)
        if not user:
            raise AuthenticationError("User not found")
        
        return self.generate_tokens(user)
    
    def revoke_token(self, token: str) -> None:
        """Revoke a token (remove from active sessions)."""
        try:
            payload = jwt.decode(
                token, self.secret_key, 
                algorithms=[self.algorithm], 
                options={"verify_exp": False}
            )
            user_id = payload.get("sub")
            if user_id:
                self.user_sessions[user_id].discard(token)
        except jwt.InvalidTokenError:
            pass  # Token already invalid
    
    def check_permission(self, user_id: str, permission: str, resource: Optional[str] = None) -> None:
        """Check if user has required permission."""
        user = self.users.get(user_id)
        if not user:
            raise AuthorizationError("User not found", user_id=user_id)
        
        if not user.has_permission(permission):
            raise AuthorizationError(
                f"Insufficient permissions. Required: {permission}",
                user_id=user_id,
                required_permission=permission,
                resource=resource
            )
    
    def get_user_from_token(self, token: str) -> User:
        """Get user object from JWT token."""
        payload = self.verify_token(token)
        user_id = payload.get("sub")
        user = self.users.get(user_id)
        
        if not user:
            raise AuthenticationError("User not found")
        
        return user
    
    def _find_user_by_username(self, username: str) -> Optional[User]:
        """Find user by username."""
        for user in self.users.values():
            if user.username == username:
                return user
        return None
    
    def _generate_user_id(self) -> str:
        """Generate unique user ID."""
        return f"user_{secrets.token_hex(8)}"
    
    def generate_api_key(self, user_id: str, name: str) -> str:
        """Generate API key for service-to-service authentication."""
        payload = {
            "sub": user_id,
            "name": name,
            "type": "api_key",
            "iat": datetime.utcnow(),
            # API keys don't expire by default
        }
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    def verify_api_key(self, api_key: str) -> Dict[str, Any]:
        """Verify API key and return payload."""
        try:
            payload = jwt.decode(api_key, self.secret_key, algorithms=[self.algorithm])
            if payload.get("type") != "api_key":
                raise AuthenticationError("Invalid API key format")
            return payload
        except jwt.InvalidTokenError as e:
            raise AuthenticationError(f"Invalid API key: {str(e)}")


# Decorators for authentication and authorization
def require_auth(security_manager: SecurityManager):
    """Decorator to require authentication."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract token from request context (implementation depends on framework)
            token = kwargs.get('token') or getattr(args[0] if args else None, 'token', None)
            if not token:
                raise AuthenticationError("Authentication token required")
            
            user = security_manager.get_user_from_token(token)
            kwargs['current_user'] = user
            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_permission(permission: str, security_manager: SecurityManager):
    """Decorator to require specific permission."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise AuthenticationError("Authentication required")
            
            security_manager.check_permission(current_user.id, permission)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def rate_limit(action: str, security_manager: SecurityManager, custom_limit: Optional[Tuple[int, int]] = None):
    """Decorator to apply rate limiting."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if current_user:
                security_manager.rate_limiter.check_limit(
                    current_user.id, action, custom_limit
                )
            return func(*args, **kwargs)
        return wrapper
    return decorator