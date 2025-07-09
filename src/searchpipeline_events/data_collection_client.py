"""
Centralized data collection client for all services.
This replaces the need for each service to implement its own DataCollectionClient.
"""

import asyncio
import time
from typing import Any, Dict, Optional
import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from .schemas import (
    BaseEvent, PatternMatchData, PatternNoMatchData, PatternLoadData,
    QueryExecutionData, QueryErrorData, SearchRequestData, ErrorData,
    ServiceName, EventType
)

logger = structlog.get_logger(__name__)


class DataCollectionClient:
    """Centralized client for sending events to the data collection service."""
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: float = 10.0,
        max_retries: int = 3
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "searchpipeline-events/1.0"
            }
            
            if self.api_key:
                headers["X-API-Key"] = self.api_key
            
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout, connect=10.0, read=10.0),
                headers=headers,
                verify=True,
                follow_redirects=True
            )
        return self._client
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5)
    )
    async def _send_event(self, event: BaseEvent) -> bool:
        """Send event to data collection service with retry logic."""
        try:
            client = await self._get_client()
            response = await client.post(
                f"{self.base_url}/collect",
                content=event.model_dump_json(),
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            return response.status_code == 200
            
        except Exception as e:
            logger.error("Failed to send event", error=str(e), event_type=event.event)
            raise
    
    async def send_event(self, event: BaseEvent) -> bool:
        """Send any event to the data collection service."""
        try:
            return await self._send_event(event)
        except Exception as e:
            logger.warning("Event delivery failed", error=str(e))
            return False
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class PatternMatcherEventClient:
    """Event client specifically for pattern matcher service."""
    
    def __init__(self, data_collection_client: DataCollectionClient):
        self.client = data_collection_client
    
    async def log_pattern_match(
        self,
        query: str,
        pattern: str,
        confidence: float,
        match_type: str = "exact",
        processing_time_ms: Optional[int] = None
    ) -> bool:
        """Log a successful pattern match."""
        event = BaseEvent(
            event=EventType.PATTERN_MATCH,
            service=ServiceName.PATTERN_MATCHER,
            data=PatternMatchData(
                query=query,
                pattern=pattern,
                confidence=confidence,
                match_type=match_type,
                processing_time_ms=processing_time_ms
            )
        )
        return await self.client.send_event(event)
    
    async def log_pattern_no_match(
        self,
        query: str,
        attempted_patterns: list = None,
        processing_time_ms: Optional[int] = None
    ) -> bool:
        """Log a failed pattern match."""
        event = BaseEvent(
            event=EventType.PATTERN_NO_MATCH,
            service=ServiceName.PATTERN_MATCHER,
            data=PatternNoMatchData(
                query=query,
                attempted_patterns=attempted_patterns or [],
                processing_time_ms=processing_time_ms
            )
        )
        return await self.client.send_event(event)
    
    async def log_pattern_load(
        self,
        pattern_count: int,
        version: str,
        load_duration_seconds: float,
        validation_error_count: int = 0
    ) -> bool:
        """Log pattern loading event."""
        event = BaseEvent(
            event=EventType.PATTERN_LOAD,
            service=ServiceName.PATTERN_MATCHER,
            data=PatternLoadData(
                pattern_count=pattern_count,
                version=version,
                load_duration_seconds=load_duration_seconds,
                validation_error_count=validation_error_count
            )
        )
        return await self.client.send_event(event)
    
    async def log_match_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Legacy method for backward compatibility.
        Converts old event format to new standardized format.
        """
        event_type = event_data.get("event_type", "pattern_match")
        
        if event_type == "pattern_match_success":
            return await self.log_pattern_match(
                query=event_data.get("query", ""),
                pattern=event_data.get("pattern_id", "unknown"),
                confidence=event_data.get("confidence", 0.0),
                match_type="exact",
                processing_time_ms=int(event_data.get("match_time_ms", 0))
            )
        elif event_type == "pattern_match_failure":
            return await self.log_pattern_no_match(
                query=event_data.get("query", ""),
                attempted_patterns=[],
                processing_time_ms=int(event_data.get("match_time_ms", 0))
            )
        else:
            logger.warning("Unknown event type", event_type=event_type)
            return False


class QueryExecutorEventClient:
    """Event client specifically for query executor service."""
    
    def __init__(self, data_collection_client: DataCollectionClient):
        self.client = data_collection_client
    
    async def log_query_execution(
        self,
        query: str,
        results_count: int,
        execution_time_ms: int,
        data_source: str,
        filters_applied: Optional[list] = None
    ) -> bool:
        """Log a successful query execution."""
        event = BaseEvent(
            event=EventType.QUERY_EXECUTION,
            service=ServiceName.QUERY_EXECUTOR,
            data=QueryExecutionData(
                query=query,
                results_count=results_count,
                execution_time_ms=execution_time_ms,
                data_source=data_source,
                filters_applied=filters_applied or []
            )
        )
        return await self.client.send_event(event)
    
    async def log_query_error(
        self,
        query: str,
        error_type: str,
        error_message: str,
        execution_time_ms: int
    ) -> bool:
        """Log a failed query execution."""
        event = BaseEvent(
            event=EventType.QUERY_ERROR,
            service=ServiceName.QUERY_EXECUTOR,
            data=QueryErrorData(
                query=query,
                error_type=error_type,
                error_message=error_message,
                execution_time_ms=execution_time_ms
            )
        )
        return await self.client.send_event(event)


class SearchGatewayEventClient:
    """Event client specifically for search gateway service."""
    
    def __init__(self, data_collection_client: DataCollectionClient):
        self.client = data_collection_client
    
    async def log_search_request(
        self,
        query: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> bool:
        """Log a search request."""
        event = BaseEvent(
            event=EventType.SEARCH_REQUEST,
            service=ServiceName.SEARCH_GATEWAY,
            data=SearchRequestData(
                query=query,
                user_id=user_id,
                session_id=session_id,
                ip_address=ip_address,
                user_agent=user_agent
            )
        )
        return await self.client.send_event(event)


class GenericEventClient:
    """Generic event client for any service."""
    
    def __init__(self, data_collection_client: DataCollectionClient, service_name: ServiceName):
        self.client = data_collection_client
        self.service_name = service_name
    
    async def log_error(
        self,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Log a generic error."""
        event = BaseEvent(
            event=EventType.ERROR,
            service=self.service_name,
            data=ErrorData(
                error_type=error_type,
                error_message=error_message,
                stack_trace=stack_trace,
                context=context or {}
            )
        )
        return await self.client.send_event(event)


# Convenience function to create a complete event logging setup
def create_event_clients(
    data_collection_url: str,
    api_key: Optional[str] = None
) -> tuple[DataCollectionClient, PatternMatcherEventClient, QueryExecutorEventClient, SearchGatewayEventClient]:
    """
    Create all event clients in one call.
    
    Returns:
        (data_collection_client, pattern_matcher_client, query_executor_client, search_gateway_client)
    """
    data_client = DataCollectionClient(data_collection_url, api_key)
    
    return (
        data_client,
        PatternMatcherEventClient(data_client),
        QueryExecutorEventClient(data_client),
        SearchGatewayEventClient(data_client)
    )