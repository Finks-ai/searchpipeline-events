"""
Tests for the EventClient class.
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch, call
import httpx
from pydantic import ValidationError

from searchpipeline_events import EventClient, ServiceName, EventType
from searchpipeline_events.schemas import create_pattern_match_event


class TestEventClient:
    """Test EventClient functionality"""
    
    @pytest.mark.asyncio
    async def test_client_initialization(self, mock_api_url):
        """Test client initialization"""
        client = EventClient(mock_api_url, ServiceName.PATTERN_MATCHER)
        
        assert client.api_url == mock_api_url
        assert client.service_name == ServiceName.PATTERN_MATCHER
        assert client.timeout == 5.0  # default
        assert client.max_retries == 3  # default
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_send_event_success(self, event_client, sample_pattern_match_data):
        """Test successful event sending"""
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            **sample_pattern_match_data
        )
        
        result = await event_client.send_event(event)
        
        assert result is True
        event_client.client.post.assert_called_once()
        
        # Check the call arguments
        call_args = event_client.client.post.call_args
        assert call_args[0][0] == event_client.api_url
        assert call_args[1]['headers']['Content-Type'] == 'application/json'
        
        # Check the JSON payload
        json_data = call_args[1]['json']
        assert json_data['event'] == 'pattern_match'
        assert json_data['service'] == 'pattern-matcher'
        assert json_data['data']['query'] == sample_pattern_match_data['query']
    
    @pytest.mark.asyncio
    async def test_send_event_http_error(self, event_client, sample_pattern_match_data):
        """Test event sending with HTTP error"""
        # Mock HTTP error response
        event_client.client.post.return_value.status_code = 500
        
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            **sample_pattern_match_data
        )
        
        result = await event_client.send_event(event)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_send_event_network_error(self, event_client, sample_pattern_match_data):
        """Test event sending with network error"""
        # Mock network error
        event_client.client.post.side_effect = httpx.RequestError("Network error")
        
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            **sample_pattern_match_data
        )
        
        result = await event_client.send_event(event)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_send_pattern_match_convenience_method(self, event_client, sample_pattern_match_data):
        """Test convenience method for pattern match events"""
        result = await event_client.send_pattern_match(**sample_pattern_match_data)
        
        assert result is True
        event_client.client.post.assert_called_once()
        
        # Check the JSON payload
        call_args = event_client.client.post.call_args
        json_data = call_args[1]['json']
        assert json_data['event'] == 'pattern_match'
        assert json_data['data']['query'] == sample_pattern_match_data['query']
    
    @pytest.mark.asyncio
    async def test_send_query_execution_convenience_method(self, event_client, sample_query_execution_data):
        """Test convenience method for query execution events"""
        result = await event_client.send_query_execution(**sample_query_execution_data)
        
        assert result is True
        event_client.client.post.assert_called_once()
        
        # Check the JSON payload
        call_args = event_client.client.post.call_args
        json_data = call_args[1]['json']
        assert json_data['event'] == 'query_execution'
        assert json_data['data']['query'] == sample_query_execution_data['query']
        assert json_data['data']['results_count'] == sample_query_execution_data['results_count']
    
    @pytest.mark.asyncio
    async def test_send_error_convenience_method(self, event_client, sample_error_data):
        """Test convenience method for error events"""
        result = await event_client.send_error(**sample_error_data)
        
        assert result is True
        event_client.client.post.assert_called_once()
        
        # Check the JSON payload
        call_args = event_client.client.post.call_args
        json_data = call_args[1]['json']
        assert json_data['event'] == 'error'
        assert json_data['data']['error_type'] == sample_error_data['error_type']
    
    @pytest.mark.asyncio
    async def test_retry_logic(self, mock_api_url, mock_httpx_client, monkeypatch):
        """Test retry logic on failure"""
        client = EventClient(mock_api_url, ServiceName.PATTERN_MATCHER, max_retries=2)
        monkeypatch.setattr(client, 'client', mock_httpx_client)
        
        # First call fails, second succeeds
        mock_httpx_client.post.side_effect = [
            httpx.RequestError("Network error"),
            AsyncMock(status_code=200)
        ]
        
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            query="test",
            pattern="test",
            confidence=0.5,
            match_type="exact"
        )
        
        result = await client.send_event(event)
        
        assert result is True
        assert mock_httpx_client.post.call_count == 2
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_queue_event(self, event_client, sample_pattern_match_data):
        """Test event queueing"""
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            **sample_pattern_match_data
        )
        
        # Queue the event
        await event_client.queue_event(event)
        
        # Wait a moment for processing
        await asyncio.sleep(0.1)
        
        # Should be queued but not sent yet (batch size not reached)
        assert len(event_client._event_queue) == 1
    
    @pytest.mark.asyncio
    async def test_batch_size_trigger(self, event_client, sample_pattern_match_data):
        """Test batch sending when batch size is reached"""
        # Set small batch size for testing
        event_client.batch_size = 2
        
        event1 = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            **sample_pattern_match_data
        )
        event2 = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            **sample_pattern_match_data
        )
        
        # Queue events
        await event_client.queue_event(event1)
        await event_client.queue_event(event2)
        
        # Wait a moment for processing
        await asyncio.sleep(0.1)
        
        # Both events should be sent
        assert event_client.client.post.call_count == 2
    
    @pytest.mark.asyncio
    async def test_context_manager(self, mock_api_url, mock_httpx_client):
        """Test async context manager"""
        async with EventClient(mock_api_url, ServiceName.PATTERN_MATCHER) as client:
            # Replace with mock
            client.client = mock_httpx_client
            
            await client.send_pattern_match(
                query="test",
                pattern="test",
                confidence=0.5,
                match_type="exact"
            )
            
            assert mock_httpx_client.post.called
        
        # Client should be closed after context manager


class TestGlobalClient:
    """Test global client functionality"""
    
    def test_init_global_client(self, mock_api_url):
        """Test global client initialization"""
        from searchpipeline_events.client import init_global_client, get_global_client
        
        client = init_global_client(mock_api_url, ServiceName.PATTERN_MATCHER)
        
        assert client is not None
        assert get_global_client() is client
        assert client.api_url == mock_api_url
        assert client.service_name == ServiceName.PATTERN_MATCHER
    
    @pytest.mark.asyncio
    async def test_send_event_global(self, mock_api_url, mock_httpx_client):
        """Test sending event with global client"""
        from searchpipeline_events.client import init_global_client, send_event_global
        
        client = init_global_client(mock_api_url, ServiceName.PATTERN_MATCHER)
        client.client = mock_httpx_client
        
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            query="test",
            pattern="test",
            confidence=0.5,
            match_type="exact"
        )
        
        result = await send_event_global(event)
        
        assert result is True
        assert mock_httpx_client.post.called
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_send_event_global_no_client(self):
        """Test sending event with no global client"""
        from searchpipeline_events.client import send_event_global, _global_client
        
        # Clear global client
        import searchpipeline_events.client
        searchpipeline_events.client._global_client = None
        
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            query="test",
            pattern="test",
            confidence=0.5,
            match_type="exact"
        )
        
        result = await send_event_global(event)
        
        assert result is False


class TestEventClientConfiguration:
    """Test client configuration options"""
    
    @pytest.mark.asyncio
    async def test_custom_timeout(self, mock_api_url):
        """Test custom timeout configuration"""
        client = EventClient(mock_api_url, ServiceName.PATTERN_MATCHER, timeout=10.0)
        
        assert client.timeout == 10.0
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_custom_batch_settings(self, mock_api_url):
        """Test custom batch settings"""
        client = EventClient(
            mock_api_url, 
            ServiceName.PATTERN_MATCHER,
            batch_size=20,
            batch_timeout=2.0
        )
        
        assert client.batch_size == 20
        assert client.batch_timeout == 2.0
        
        await client.close()