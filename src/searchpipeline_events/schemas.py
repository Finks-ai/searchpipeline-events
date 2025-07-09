"""
Event schemas for the search pipeline data collection service.
This module defines standardized event structures using Pydantic models.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator, ConfigDict


class EventType(str, Enum):
    """Standardized event types across all services"""
    # Pattern Matcher Events
    PATTERN_MATCH = "pattern_match"
    PATTERN_NO_MATCH = "pattern_no_match"
    PATTERN_LOAD = "pattern_load"
    
    # Query Executor Events
    QUERY_EXECUTION = "query_execution"
    QUERY_ERROR = "query_error"
    
    # Query Interpreter Events
    QUERY_INTERPRETATION = "query_interpretation"
    
    # Search Gateway Events
    SEARCH_REQUEST = "search_request"
    RATE_LIMIT_HIT = "rate_limit_hit"
    
    # Generic Events
    SERVICE_START = "service_start"
    SERVICE_STOP = "service_stop"
    ERROR = "error"


class ServiceName(str, Enum):
    """Standardized service names"""
    SEARCH_GATEWAY = "search-gateway"
    PATTERN_MATCHER = "pattern-matcher"
    QUERY_INTERPRETER = "query-interpreter"
    QUERY_EXECUTOR = "query-executor"
    DATA_COLLECTION = "data-collection"
    ETL_PIPELINE = "etl-pipeline"


class BaseEventData(BaseModel):
    """Base class for all event data"""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processing_time_ms: Optional[int] = Field(None, ge=0)
    
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )


class PatternMatchData(BaseEventData):
    """Data for pattern match events"""
    query: str = Field(..., min_length=1)
    pattern: str = Field(..., min_length=1)
    confidence: float = Field(..., ge=0.0, le=1.0)
    match_type: str = Field(..., pattern="^(exact|fuzzy|semantic)$")
    confidence_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)
    closest_matches: Optional[List[Dict[str, Any]]] = Field(default_factory=list)


class PatternNoMatchData(BaseEventData):
    """Data for pattern no match events"""
    query: str = Field(..., min_length=1)
    confidence_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)
    closest_matches: Optional[List[Dict[str, Any]]] = Field(default_factory=list)


class PatternLoadData(BaseEventData):
    """Data for pattern load events"""
    pattern_count: int = Field(..., ge=0)
    version: str = Field(..., min_length=1)
    load_duration_seconds: float = Field(..., ge=0.0)
    validation_error_count: int = Field(..., ge=0)


class QueryExecutionData(BaseEventData):
    """Data for successful query execution events"""
    query: str = Field(..., min_length=1)
    results_count: int = Field(..., ge=0)
    execution_time_ms: int = Field(..., ge=0)
    data_source: str = Field(..., min_length=1)
    filters_applied: Optional[List[str]] = Field(default_factory=list)


class QueryErrorData(BaseEventData):
    """Data for query execution error events"""
    query: str = Field(..., min_length=1)
    error_type: str = Field(..., pattern="^(timeout|connection|validation|unknown)$")
    error_message: str = Field(..., min_length=1)
    execution_time_ms: int = Field(..., ge=0)


class QueryInterpretationData(BaseEventData):
    """Data for query interpretation events"""
    original_query: str = Field(..., min_length=1)
    interpreted_query: str = Field(..., min_length=1)
    interpretation_confidence: float = Field(..., ge=0.0, le=1.0)


class SearchRequestData(BaseEventData):
    """Data for search request events"""
    query: str = Field(..., min_length=1)
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None


class RateLimitHitData(BaseEventData):
    """Data for rate limit hit events"""
    user_id: Optional[str] = None
    ip_address: Optional[str] = None
    limit_type: str = Field(..., min_length=1)
    current_count: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)


class ServiceLifecycleData(BaseEventData):
    """Data for service lifecycle events"""
    service_version: Optional[str] = None
    environment: Optional[str] = None
    startup_time_ms: Optional[int] = Field(None, ge=0)


class ErrorData(BaseEventData):
    """Data for generic error events"""
    error_type: str = Field(..., min_length=1)
    error_message: str = Field(..., min_length=1)
    stack_trace: Optional[str] = None
    context: Optional[Dict[str, Any]] = Field(default_factory=dict)


class BaseEvent(BaseModel):
    """Base event structure"""
    event: EventType
    service: ServiceName
    data: Union[
        PatternMatchData,
        PatternNoMatchData,
        PatternLoadData,
        QueryExecutionData,
        QueryErrorData,
        QueryInterpretationData,
        SearchRequestData,
        RateLimitHitData,
        ServiceLifecycleData,
        ErrorData,
        Dict[str, Any]  # Fallback for custom data
    ]
    
    @field_validator('data', mode='before')
    @classmethod
    def validate_data_type(cls, v, info):
        """Validate that data matches the event type"""
        # For Pydantic v2, we need to check context differently
        if not hasattr(info, 'context') or not info.context:
            return v
            
        # Try to get event type from context or field values
        event_type = None
        if hasattr(info, 'data') and info.data and 'event' in info.data:
            event_type = info.data['event']
        
        if not event_type:
            return v
            
        data_type_map = {
            EventType.PATTERN_MATCH: PatternMatchData,
            EventType.PATTERN_NO_MATCH: PatternNoMatchData,
            EventType.PATTERN_LOAD: PatternLoadData,
            EventType.QUERY_EXECUTION: QueryExecutionData,
            EventType.QUERY_ERROR: QueryErrorData,
            EventType.QUERY_INTERPRETATION: QueryInterpretationData,
            EventType.SEARCH_REQUEST: SearchRequestData,
            EventType.RATE_LIMIT_HIT: RateLimitHitData,
            EventType.SERVICE_START: ServiceLifecycleData,
            EventType.SERVICE_STOP: ServiceLifecycleData,
            EventType.ERROR: ErrorData,
        }
        
        expected_type = data_type_map.get(event_type)
        if expected_type and not isinstance(v, expected_type):
            # If it's a dict, try to convert it
            if isinstance(v, dict):
                try:
                    return expected_type(**v)
                except Exception:
                    pass
            # If conversion fails, allow it but log a warning
            pass
        
        return v


# Event factory functions for easy creation
def create_pattern_match_event(
    service: ServiceName,
    query: str,
    pattern: str,
    confidence: float,
    match_type: str,
    processing_time_ms: Optional[int] = None,
    confidence_threshold: Optional[float] = None,
    closest_matches: Optional[List[Dict[str, Any]]] = None
) -> BaseEvent:
    """Create a pattern match event"""
    return BaseEvent(
        event=EventType.PATTERN_MATCH,
        service=service,
        data=PatternMatchData(
            query=query,
            pattern=pattern,
            confidence=confidence,
            match_type=match_type,
            processing_time_ms=processing_time_ms,
            confidence_threshold=confidence_threshold,
            closest_matches=closest_matches or []
        )
    )


def create_pattern_no_match_event(
    service: ServiceName,
    query: str,
    processing_time_ms: Optional[int] = None,
    confidence_threshold: Optional[float] = None,
    closest_matches: Optional[List[Dict[str, Any]]] = None
) -> BaseEvent:
    """Create a pattern no match event"""
    return BaseEvent(
        event=EventType.PATTERN_NO_MATCH,
        service=service,
        data=PatternNoMatchData(
            query=query,
            processing_time_ms=processing_time_ms,
            confidence_threshold=confidence_threshold,
            closest_matches=closest_matches or []
        )
    )


def create_pattern_load_event(
    service: ServiceName,
    pattern_count: int,
    version: str,
    load_duration_seconds: float,
    validation_error_count: int = 0
) -> BaseEvent:
    """Create a pattern load event"""
    return BaseEvent(
        event=EventType.PATTERN_LOAD,
        service=service,
        data=PatternLoadData(
            pattern_count=pattern_count,
            version=version,
            load_duration_seconds=load_duration_seconds,
            validation_error_count=validation_error_count
        )
    )


def create_query_execution_event(
    service: ServiceName,
    query: str,
    results_count: int,
    execution_time_ms: int,
    data_source: str,
    filters_applied: Optional[List[str]] = None
) -> BaseEvent:
    """Create a query execution event"""
    return BaseEvent(
        event=EventType.QUERY_EXECUTION,
        service=service,
        data=QueryExecutionData(
            query=query,
            results_count=results_count,
            execution_time_ms=execution_time_ms,
            data_source=data_source,
            filters_applied=filters_applied or []
        )
    )


def create_query_error_event(
    service: ServiceName,
    query: str,
    error_type: str,
    error_message: str,
    execution_time_ms: int
) -> BaseEvent:
    """Create a query error event"""
    return BaseEvent(
        event=EventType.QUERY_ERROR,
        service=service,
        data=QueryErrorData(
            query=query,
            error_type=error_type,
            error_message=error_message,
            execution_time_ms=execution_time_ms
        )
    )


def create_search_request_event(
    service: ServiceName,
    query: str,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> BaseEvent:
    """Create a search request event"""
    return BaseEvent(
        event=EventType.SEARCH_REQUEST,
        service=service,
        data=SearchRequestData(
            query=query,
            user_id=user_id,
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent
        )
    )


def create_query_interpretation_event(
    service: ServiceName,
    original_query: str,
    interpreted_query: str,
    interpretation_confidence: float,
    processing_time_ms: Optional[int] = None
) -> BaseEvent:
    """Create a query interpretation event"""
    return BaseEvent(
        event=EventType.QUERY_INTERPRETATION,
        service=service,
        data=QueryInterpretationData(
            original_query=original_query,
            interpreted_query=interpreted_query,
            interpretation_confidence=interpretation_confidence,
            processing_time_ms=processing_time_ms
        )
    )


def create_rate_limit_hit_event(
    service: ServiceName,
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    limit_type: str = "requests_per_minute",
    current_count: int = 0,
    limit: int = 60
) -> BaseEvent:
    """Create a rate limit hit event"""
    return BaseEvent(
        event=EventType.RATE_LIMIT_HIT,
        service=service,
        data=RateLimitHitData(
            user_id=user_id,
            ip_address=ip_address,
            limit_type=limit_type,
            current_count=current_count,
            limit=limit
        )
    )


def create_error_event(
    service: ServiceName,
    error_type: str,
    error_message: str,
    stack_trace: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> BaseEvent:
    """Create a generic error event"""
    return BaseEvent(
        event=EventType.ERROR,
        service=service,
        data=ErrorData(
            error_type=error_type,
            error_message=error_message,
            stack_trace=stack_trace,
            context=context or {}
        )
    )


# JSON Schema generation for documentation
def generate_event_schemas() -> Dict[str, Any]:
    """Generate JSON schemas for all event types"""
    return {
        "base_event": BaseEvent.schema(),
        "event_types": {event.value: event.value for event in EventType},
        "service_names": {service.value: service.value for service in ServiceName},
        "data_schemas": {
            "pattern_match": PatternMatchData.schema(),
            "pattern_no_match": PatternNoMatchData.schema(),
            "pattern_load": PatternLoadData.schema(),
            "query_execution": QueryExecutionData.schema(),
            "query_error": QueryErrorData.schema(),
            "query_interpretation": QueryInterpretationData.schema(),
            "search_request": SearchRequestData.schema(),
            "rate_limit_hit": RateLimitHitData.schema(),
            "service_lifecycle": ServiceLifecycleData.schema(),
            "error": ErrorData.schema(),
        }
    }


if __name__ == "__main__":
    import json
    
    # Generate and print schemas
    schemas = generate_event_schemas()
    print(json.dumps(schemas, indent=2))