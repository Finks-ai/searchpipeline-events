"""
Service-specific event client wrappers.
These provide convenient methods for each service type.
"""

from typing import List, Optional

from .client import EventClient
from .schemas import ServiceName


class PatternMatcherClient:
    """Event client for pattern matcher service"""
    
    def __init__(self, api_url: str, **kwargs):
        self.client = EventClient(api_url, ServiceName.PATTERN_MATCHER, **kwargs)
    
    async def pattern_found(self, query: str, pattern: str, confidence: float, 
                          match_type: str, processing_time_ms: int) -> bool:
        """Log when a pattern is matched"""
        return await self.client.send_pattern_match(
            query=query,
            pattern=pattern,
            confidence=confidence,
            match_type=match_type,
            processing_time_ms=processing_time_ms
        )
    
    async def pattern_not_found(self, query: str, attempted_patterns: List[str], 
                               processing_time_ms: int) -> bool:
        """Log when no pattern is matched"""
        return await self.client.send_pattern_no_match(
            query=query,
            attempted_patterns=attempted_patterns,
            processing_time_ms=processing_time_ms
        )
    
    async def patterns_loaded(self, pattern_count: int, version: str, 
                             load_duration_seconds: float, 
                             validation_error_count: int = 0) -> bool:
        """Log when patterns are loaded"""
        return await self.client.send_pattern_load(
            pattern_count=pattern_count,
            version=version,
            load_duration_seconds=load_duration_seconds,
            validation_error_count=validation_error_count
        )
    
    async def close(self):
        """Close the client"""
        await self.client.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class QueryExecutorClient:
    """Event client for query executor service"""
    
    def __init__(self, api_url: str, **kwargs):
        self.client = EventClient(api_url, ServiceName.QUERY_EXECUTOR, **kwargs)
    
    async def query_executed(self, query: str, results_count: int, 
                           execution_time_ms: int, data_source: str,
                           filters_applied: Optional[List[str]] = None) -> bool:
        """Log successful query execution"""
        return await self.client.send_query_execution(
            query=query,
            results_count=results_count,
            execution_time_ms=execution_time_ms,
            data_source=data_source,
            filters_applied=filters_applied
        )
    
    async def query_failed(self, query: str, error_type: str, error_message: str,
                         execution_time_ms: int) -> bool:
        """Log failed query execution"""
        return await self.client.send_query_error(
            query=query,
            error_type=error_type,
            error_message=error_message,
            execution_time_ms=execution_time_ms
        )
    
    async def close(self):
        """Close the client"""
        await self.client.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class QueryInterpreterClient:
    """Event client for query interpreter service"""
    
    def __init__(self, api_url: str, **kwargs):
        self.client = EventClient(api_url, ServiceName.QUERY_INTERPRETER, **kwargs)
    
    async def query_interpreted(self, original_query: str, interpreted_query: str,
                              interpretation_confidence: float, 
                              processing_time_ms: int) -> bool:
        """Log successful query interpretation"""
        from .schemas import create_query_interpretation_event
        
        event = create_query_interpretation_event(
            service=ServiceName.QUERY_INTERPRETER,
            original_query=original_query,
            interpreted_query=interpreted_query,
            interpretation_confidence=interpretation_confidence,
            processing_time_ms=processing_time_ms
        )
        return await self.client.send_event(event)
    
    async def close(self):
        """Close the client"""
        await self.client.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class SearchGatewayClient:
    """Event client for search gateway service"""
    
    def __init__(self, api_url: str, **kwargs):
        self.client = EventClient(api_url, ServiceName.SEARCH_GATEWAY, **kwargs)
    
    async def search_requested(self, query: str, user_id: Optional[str] = None,
                             session_id: Optional[str] = None, 
                             ip_address: Optional[str] = None,
                             user_agent: Optional[str] = None) -> bool:
        """Log search request"""
        return await self.client.send_search_request(
            query=query,
            user_id=user_id,
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent
        )
    
    async def rate_limit_hit(self, user_id: Optional[str] = None,
                           ip_address: Optional[str] = None,
                           limit_type: str = "requests_per_minute",
                           current_count: int = 0, limit: int = 60) -> bool:
        """Log rate limit hit"""
        from .schemas import create_rate_limit_hit_event
        
        event = create_rate_limit_hit_event(
            service=ServiceName.SEARCH_GATEWAY,
            user_id=user_id,
            ip_address=ip_address,
            limit_type=limit_type,
            current_count=current_count,
            limit=limit
        )
        return await self.client.send_event(event)
    
    async def close(self):
        """Close the client"""
        await self.client.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()