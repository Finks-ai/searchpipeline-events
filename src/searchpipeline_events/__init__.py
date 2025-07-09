"""
Search Pipeline Events - Standardized event collection for search pipeline services.
"""

from .client import EventClient, init_global_client, get_global_client
from .clients import (
    PatternMatcherClient,
    QueryExecutorClient,
    QueryInterpreterClient,
    SearchGatewayClient,
)
from .decorators import (
    track_execution,
    track_query_execution,
    track_pattern_matching,
    get_query_from_first_arg,
    get_results_count_from_list,
    get_pattern_info_from_result,
)
from .schemas import (
    BaseEvent,
    EventType,
    ServiceName,
    # Event data classes
    PatternMatchData,
    PatternNoMatchData,
    QueryExecutionData,
    QueryErrorData,
    QueryInterpretationData,
    SearchRequestData,
    RateLimitHitData,
    ServiceLifecycleData,
    ErrorData,
    # Event creation functions
    create_pattern_match_event,
    create_pattern_no_match_event,
    create_query_execution_event,
    create_query_error_event,
    create_query_interpretation_event,
    create_search_request_event,
    create_rate_limit_hit_event,
    create_error_event,
)

__version__ = "0.1.0"
__all__ = [
    # Main client
    "EventClient",
    "init_global_client",
    "get_global_client",
    
    # Service-specific clients
    "PatternMatcherClient",
    "QueryExecutorClient", 
    "QueryInterpreterClient",
    "SearchGatewayClient",
    
    # Decorators
    "track_execution",
    "track_query_execution",
    "track_pattern_matching",
    "get_query_from_first_arg",
    "get_results_count_from_list",
    "get_pattern_info_from_result",
    
    # Schemas
    "BaseEvent",
    "EventType",
    "ServiceName",
    "PatternMatchData",
    "PatternNoMatchData",
    "QueryExecutionData",
    "QueryErrorData",
    "QueryInterpretationData",
    "SearchRequestData",
    "RateLimitHitData",
    "ServiceLifecycleData",
    "ErrorData",
    
    # Event creation
    "create_pattern_match_event",
    "create_pattern_no_match_event",
    "create_query_execution_event",
    "create_query_error_event",
    "create_query_interpretation_event",
    "create_search_request_event",
    "create_rate_limit_hit_event",
    "create_error_event",
]