"""
Event client for sending structured events to the data collection service.
This module provides a type-safe way to send events with automatic validation.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import httpx
from pydantic import ValidationError

from .schemas import (
    BaseEvent,
    EventType,
    ServiceName,
    create_pattern_match_event,
    create_pattern_no_match_event,
    create_query_execution_event,
    create_query_error_event,
    create_search_request_event,
    create_error_event,
)


logger = logging.getLogger(__name__)


class EventClient:
    """Type-safe event client with automatic validation and retry logic"""
    
    def __init__(
        self,
        api_url: str,
        service_name: ServiceName,
        timeout: float = 5.0,
        max_retries: int = 3,
        batch_size: int = 10,
        batch_timeout: float = 1.0
    ):
        """
        Initialize the event client
        
        Args:
            api_url: URL of the data collection API
            service_name: Name of the service sending events
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            batch_size: Maximum number of events to batch together
            batch_timeout: Maximum time to wait before sending a batch
        """
        self.api_url = api_url
        self.service_name = service_name
        self.timeout = timeout
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        self.client = httpx.AsyncClient(timeout=timeout)
        self._event_queue: List[BaseEvent] = []
        self._batch_task: Optional[asyncio.Task] = None
        self._shutdown = False
    
    async def send_event(self, event: BaseEvent) -> bool:
        """
        Send a single event immediately
        
        Args:
            event: The event to send
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate event
            event_dict = event.model_dump()
            
            # Send to API
            response = await self._send_with_retry(event_dict)
            return response.status_code == 200
            
        except ValidationError as e:
            logger.error(f"Event validation failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            return False
    
    async def queue_event(self, event: BaseEvent) -> None:
        """
        Queue an event for batch sending
        
        Args:
            event: The event to queue
        """
        if self._shutdown:
            return
            
        self._event_queue.append(event)
        
        # Start batch processing if not already running
        if self._batch_task is None or self._batch_task.done():
            self._batch_task = asyncio.create_task(self._batch_processor())
        
        # Send immediately if queue is full
        if len(self._event_queue) >= self.batch_size:
            await self._flush_queue()
    
    async def _batch_processor(self) -> None:
        """Background task to process batched events"""
        while not self._shutdown:
            try:
                await asyncio.sleep(self.batch_timeout)
                if self._event_queue:
                    await self._flush_queue()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
    
    async def _flush_queue(self) -> None:
        """Flush all queued events"""
        if not self._event_queue:
            return
            
        events_to_send = self._event_queue[:self.batch_size]
        self._event_queue = self._event_queue[self.batch_size:]
        
        # Send batch
        for event in events_to_send:
            try:
                await self.send_event(event)
            except Exception as e:
                logger.error(f"Failed to send queued event: {e}")
    
    async def _send_with_retry(self, event_data: Dict[str, Any]) -> httpx.Response:
        """Send event with retry logic"""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                response = await self.client.post(
                    self.api_url,
                    json=event_data,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code >= 500:
                    # Server error, retry
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                
                return response
                
            except httpx.RequestError as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
        
        # If we get here, all retries failed
        raise last_exception or Exception("Max retries exceeded")
    
    # Convenience methods for common event types
    async def send_pattern_match(
        self,
        query: str,
        pattern: str,
        confidence: float,
        match_type: str,
        processing_time_ms: Optional[int] = None
    ) -> bool:
        """Send a pattern match event"""
        event = create_pattern_match_event(
            service=self.service_name,
            query=query,
            pattern=pattern,
            confidence=confidence,
            match_type=match_type,
            processing_time_ms=processing_time_ms
        )
        return await self.send_event(event)
    
    async def send_pattern_no_match(
        self,
        query: str,
        attempted_patterns: List[str],
        processing_time_ms: Optional[int] = None
    ) -> bool:
        """Send a pattern no match event"""
        event = create_pattern_no_match_event(
            service=self.service_name,
            query=query,
            attempted_patterns=attempted_patterns,
            processing_time_ms=processing_time_ms
        )
        return await self.send_event(event)
    
    async def send_query_execution(
        self,
        query: str,
        results_count: int,
        execution_time_ms: int,
        data_source: str,
        filters_applied: Optional[List[str]] = None
    ) -> bool:
        """Send a query execution event"""
        event = create_query_execution_event(
            service=self.service_name,
            query=query,
            results_count=results_count,
            execution_time_ms=execution_time_ms,
            data_source=data_source,
            filters_applied=filters_applied
        )
        return await self.send_event(event)
    
    async def send_query_error(
        self,
        query: str,
        error_type: str,
        error_message: str,
        execution_time_ms: int
    ) -> bool:
        """Send a query error event"""
        event = create_query_error_event(
            service=self.service_name,
            query=query,
            error_type=error_type,
            error_message=error_message,
            execution_time_ms=execution_time_ms
        )
        return await self.send_event(event)
    
    async def send_search_request(
        self,
        query: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> bool:
        """Send a search request event"""
        event = create_search_request_event(
            service=self.service_name,
            query=query,
            user_id=user_id,
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent
        )
        return await self.send_event(event)
    
    async def send_error(
        self,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Send an error event"""
        event = create_error_event(
            service=self.service_name,
            error_type=error_type,
            error_message=error_message,
            stack_trace=stack_trace,
            context=context
        )
        return await self.send_event(event)
    
    async def close(self) -> None:
        """Close the client and flush any remaining events"""
        self._shutdown = True
        
        # Cancel batch processor
        if self._batch_task and not self._batch_task.done():
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass
        
        # Flush remaining events
        await self._flush_queue()
        
        # Close HTTP client
        await self.client.aclose()
    
    async def __aenter__(self):
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()


# Singleton pattern for global event client
_global_client: Optional[EventClient] = None


def init_global_client(
    api_url: str,
    service_name: ServiceName,
    **kwargs
) -> EventClient:
    """Initialize the global event client"""
    global _global_client
    _global_client = EventClient(api_url, service_name, **kwargs)
    return _global_client


def get_global_client() -> Optional[EventClient]:
    """Get the global event client"""
    return _global_client


async def send_event_global(event: BaseEvent) -> bool:
    """Send an event using the global client"""
    if _global_client is None:
        logger.warning("Global event client not initialized")
        return False
    return await _global_client.send_event(event)


# Example usage and testing
if __name__ == "__main__":
    async def test_client():
        """Test the event client"""
        client = EventClient(
            api_url="https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect",
            service_name=ServiceName.PATTERN_MATCHER
        )
        
        # Test pattern match event
        success = await client.send_pattern_match(
            query="Apple stock price",
            pattern="financial_data",
            confidence=0.95,
            match_type="exact",
            processing_time_ms=45
        )
        
        print(f"Pattern match event sent: {success}")
        
        # Test error event
        success = await client.send_error(
            error_type="validation",
            error_message="Invalid query format",
            context={"query": "invalid query"}
        )
        
        print(f"Error event sent: {success}")
        
        await client.close()
    
    asyncio.run(test_client())