import os
import json
import pickle
import hashlib
import functools
import inspect
from typing import Any, Dict, List, Optional, Tuple, Callable, TypeVar, Union, cast
from datetime import datetime, timedelta
import redis
from redis.exceptions import RedisError
import logging

logger = logging.getLogger(__name__)

# Type variables for function signatures
T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
REDIS_SSL = os.getenv("REDIS_SSL", "false").lower() == "true"
CACHE_PREFIX = os.getenv("CACHE_PREFIX", "scheduler:")
CACHE_DEFAULT_TTL = int(os.getenv("CACHE_DEFAULT_TTL", "3600"))  # 1 hour

class RedisCache:
    """
    A wrapper around Redis for caching purposes.
    Provides methods for storing, retrieving, and invalidating cached data.
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """Implement the Singleton pattern."""
        if cls._instance is None:
            cls._instance = super(RedisCache, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        password: Optional[str] = REDIS_PASSWORD,
        ssl: bool = REDIS_SSL,
        prefix: str = CACHE_PREFIX,
        default_ttl: int = CACHE_DEFAULT_TTL,
    ):
        """Initialize the Redis cache connection."""
        # Skip initialization if already initialized (Singleton pattern)
        if getattr(self, '_initialized', False):
            return
        
        self.prefix = prefix
        self.default_ttl = default_ttl
        
        # Create Redis connection pool
        pool_kwargs = {
            "host": host,
            "port": port,
            "db": db,
            "decode_responses": False,  # We'll handle decoding ourselves
        }
        
        if password:
            pool_kwargs["password"] = password
            
        if ssl:
            pool_kwargs["ssl"] = True
            pool_kwargs["ssl_cert_reqs"] = None
        
        try:
            self.redis = redis.Redis(**pool_kwargs)
            # Test connection
            self.redis.ping()
            logger.info(f"Connected to Redis at {host}:{port}/{db}")
        except RedisError as e:
            logger.warning(f"Failed to connect to Redis: {str(e)}. Caching will be disabled.")
            self.redis = None
        
        self._initialized = True
    
    def _get_key(self, key: str) -> str:
        """
        Prepend the cache prefix to the key.
        
        Args:
            key: The original key
            
        Returns:
            The key with the prefix prepended
        """
        return f"{self.prefix}{key}"
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key: The cache key
            
        Returns:
            The cached value, or None if not found
        """
        if not self.redis:
            return None
            
        try:
            # Get the value from Redis
            cached_value = self.redis.get(self._get_key(key))
            
            if cached_value is None:
                return None
                
            # Deserialize the value
            return pickle.loads(cached_value)
        except Exception as e:
            logger.warning(f"Error retrieving from cache: {str(e)}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a value in the cache.
        
        Args:
            key: The cache key
            value: The value to cache
            ttl: Time-to-live in seconds, or None for default TTL
            
        Returns:
            True if successful, False otherwise
        """
        if not self.redis:
            return False
            
        try:
            # Serialize the value
            serialized_value = pickle.dumps(value)
            
            # Set in Redis with TTL
            self.redis.setex(
                self._get_key(key),
                ttl if ttl is not None else self.default_ttl,
                serialized_value
            )
            return True
        except Exception as e:
            logger.warning(f"Error setting cache: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: The cache key
            
        Returns:
            True if successful, False otherwise
        """
        if not self.redis:
            return False
            
        try:
            self.redis.delete(self._get_key(key))
            return True
        except Exception as e:
            logger.warning(f"Error deleting from cache: {str(e)}")
            return False
    
    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.
        
        Args:
            pattern: The pattern to match (e.g., "user:*")
            
        Returns:
            The number of keys deleted
        """
        if not self.redis:
            return 0
            
        try:
            # Get all keys matching the pattern
            keys = self.redis.keys(self._get_key(pattern))
            
            if not keys:
                return 0
                
            # Delete all matching keys
            return self.redis.delete(*keys)
        except Exception as e:
            logger.warning(f"Error deleting pattern from cache: {str(e)}")
            return 0
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.
        
        Args:
            key: The cache key
            
        Returns:
            True if the key exists, False otherwise
        """
        if not self.redis:
            return False
            
        try:
            return bool(self.redis.exists(self._get_key(key)))
        except Exception as e:
            logger.warning(f"Error checking cache existence: {str(e)}")
            return False
    
    def ttl(self, key: str) -> Optional[int]:
        """
        Get the remaining TTL for a key.
        
        Args:
            key: The cache key
            
        Returns:
            The remaining TTL in seconds, or None if the key doesn't exist
        """
        if not self.redis:
            return None
            
        try:
            ttl = self.redis.ttl(self._get_key(key))
            return ttl if ttl > 0 else None
        except Exception as e:
            logger.warning(f"Error getting cache TTL: {str(e)}")
            return None
    
    def clear_all(self) -> bool:
        """
        Clear all keys with the cache prefix.
        
        Returns:
            True if successful, False otherwise
        """
        return self.delete_pattern("*") > 0
    
    def health_check(self) -> bool:
        """
        Check if the Redis connection is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        if not self.redis:
            return False
            
        try:
            return bool(self.redis.ping())
        except Exception:
            return False

def _create_cache_key(
    prefix: str,
    func: Callable,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    ignore_args: Optional[List[int]] = None,
    ignore_kwargs: Optional[List[str]] = None,
) -> str:
    """
    Create a cache key based on the function and its arguments.
    
    Args:
        prefix: The prefix for the cache key
        func: The function being cached
        args: The positional arguments
        kwargs: The keyword arguments
        ignore_args: Indices of positional arguments to ignore
        ignore_kwargs: Names of keyword arguments to ignore
        
    Returns:
        A cache key string
    """
    # Get the fully qualified function name
    func_name = f"{func.__module__}.{func.__qualname__}"
    
    # Filter arguments
    filtered_args = list(args)
    if ignore_args:
        for idx in sorted(ignore_args, reverse=True):
            if idx < len(filtered_args):
                filtered_args.pop(idx)
    
    filtered_kwargs = kwargs.copy()
    if ignore_kwargs:
        for key in ignore_kwargs:
            filtered_kwargs.pop(key, None)
    
    # Create a string representation of the arguments
    args_str = str(filtered_args) if filtered_args else ""
    kwargs_str = str(sorted(filtered_kwargs.items())) if filtered_kwargs else ""
    
    # Create a hash of the arguments
    args_hash = hashlib.md5(f"{args_str}{kwargs_str}".encode()).hexdigest()
    
    # Construct the final cache key
    return f"{prefix}{func_name}:{args_hash}"

def cache_result(
    ttl: Optional[int] = None,
    prefix: Optional[str] = None,
    ignore_args: Optional[List[int]] = None,
    ignore_kwargs: Optional[List[str]] = None,
) -> Callable[[F], F]:
    """
    Decorator to cache function results in Redis.
    
    Args:
        ttl: Time-to-live in seconds, or None for default TTL
        prefix: Custom prefix for the cache key
        ignore_args: Indices of positional arguments to ignore in the cache key
        ignore_kwargs: Names of keyword arguments to ignore in the cache key
        
    Returns:
        A decorator function
    """
    cache = RedisCache()
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Skip caching if Redis is not available
            if not cache.redis:
                return func(*args, **kwargs)
            
            # Determine the cache key
            key_prefix = prefix or f"cache:{func.__module__}:"
            cache_key = _create_cache_key(
                key_prefix, func, args, kwargs, ignore_args, ignore_kwargs
            )
            
            # Check if the result is already cached
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_result
            
            # Execute the function and cache the result
            logger.debug(f"Cache miss for {cache_key}")
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            
            return result
        
        return cast(F, wrapper)
    
    return decorator

def invalidate_cache(
    prefix: Optional[str] = None,
    func: Optional[Callable] = None,
    args: Optional[Tuple[Any, ...]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    pattern: Optional[str] = None,
) -> bool:
    """
    Invalidate cached results.
    
    Args:
        prefix: Custom prefix for the cache key
        func: The function whose cache to invalidate
        args: The positional arguments (if invalidating a specific call)
        kwargs: The keyword arguments (if invalidating a specific call)
        pattern: A pattern to match cache keys (e.g., "user:*")
        
    Returns:
        True if successful, False otherwise
    """
    cache = RedisCache()
    
    # If no Redis connection, return False
    if not cache.redis:
        return False
    
    # If a pattern is provided, delete by pattern
    if pattern:
        return cache.delete_pattern(pattern) > 0
    
    # If a function is provided
    if func:
        # Determine the cache key
        key_prefix = prefix or f"cache:{func.__module__}:"
        
        # If args and kwargs are provided, invalidate a specific call
        if args is not None and kwargs is not None:
            cache_key = _create_cache_key(key_prefix, func, args, kwargs)
            return cache.delete(cache_key)
        
        # Otherwise, invalidate all calls to the function
        return cache.delete_pattern(f"{key_prefix}{func.__module__}.{func.__qualname__}:*")
    
    # If only a prefix is provided, delete all keys with that prefix
    if prefix:
        return cache.delete_pattern(f"{prefix}*")
    
    return False