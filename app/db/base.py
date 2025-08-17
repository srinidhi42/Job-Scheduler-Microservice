from typing import Any, Dict, List, Optional, TypeVar, Generic, Type, ClassVar
from datetime import datetime
from sqlalchemy import Column, Integer, DateTime, func, inspect
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Session

# Create declarative base for SQLAlchemy models
Base = declarative_base()

# Type variables for generic model operations
T = TypeVar("T", bound="Base")

class TimestampMixin:
    """Mixin to add created_at and updated_at columns to a model."""
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class ModelMixin(Generic[T]):
    """Mixin to add common model operations to a model."""
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    @classmethod
    def get_by_id(cls: Type[T], db: Session, id: int) -> Optional[T]:
        """Get a record by ID."""
        return db.query(cls).filter(cls.id == id).first()
    
    @classmethod
    def get_all(cls: Type[T], db: Session, skip: int = 0, limit: int = 100) -> List[T]:
        """Get all records with pagination."""
        return db.query(cls).offset(skip).limit(limit).all()
    
    @classmethod
    def create(cls: Type[T], db: Session, **kwargs) -> T:
        """Create a new record."""
        obj = cls(**kwargs)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return obj
    
    @classmethod
    def update(cls: Type[T], db: Session, id: int, **kwargs) -> Optional[T]:
        """Update a record by ID."""
        obj = cls.get_by_id(db, id)
        if not obj:
            return None
            
        for key, value in kwargs.items():
            if hasattr(obj, key):
                setattr(obj, key, value)
                
        db.commit()
        db.refresh(obj)
        return obj
    
    @classmethod
    def delete(cls: Type[T], db: Session, id: int) -> bool:
        """Delete a record by ID."""
        obj = cls.get_by_id(db, id)
        if not obj:
            return False
            
        db.delete(obj)
        db.commit()
        return True
    
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create a model instance from a dictionary."""
        return cls(**{
            k: v for k, v in data.items() 
            if k in inspect(cls).columns.keys() or hasattr(cls, k)
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the model instance to a dictionary."""
        result = {}
        for key in inspect(self.__class__).columns.keys():
            value = getattr(self, key)
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result
    
    def update_from_dict(self, data: Dict[str, Any]) -> None:
        """Update the model instance from a dictionary."""
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def __repr__(self) -> str:
        """String representation of the model instance."""
        values = ", ".join(
            f"{key}={getattr(self, key)}" 
            for key in inspect(self.__class__).columns.keys()
            if key in ["id", "name"] or (hasattr(self, "display_key") and key == self.display_key)
        )
        return f"<{self.__class__.__name__}({values})>"


class AuditableMixin:
    """Mixin to add auditing fields to a model."""
    
    created_by = Column(Integer, nullable=True)
    updated_by = Column(Integer, nullable=True)
    
    @hybrid_property
    def is_deleted(self) -> bool:
        """Check if the record is deleted (if the model has a deleted column)."""
        return hasattr(self, "deleted") and bool(getattr(self, "deleted"))


class SoftDeleteMixin:
    """Mixin to add soft delete capability to a model."""
    
    deleted = Column(DateTime, nullable=True)
    
    @hybrid_property
    def is_deleted(self) -> bool:
        """Check if the record is deleted."""
        return self.deleted is not None
    
    def soft_delete(self, db: Session) -> None:
        """Soft delete the record."""
        self.deleted = func.now()
        db.commit()
    
    def restore(self, db: Session) -> None:
        """Restore a soft-deleted record."""
        self.deleted = None
        db.commit()
    
    @classmethod
    def get_active(cls: Type[T], db: Session, skip: int = 0, limit: int = 100) -> List[T]:
        """Get all active (not deleted) records with pagination."""
        return db.query(cls).filter(cls.deleted.is_(None)).offset(skip).limit(limit).all()