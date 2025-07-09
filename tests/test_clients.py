"""
Tests for service-specific client wrappers.
"""

import pytest
from unittest.mock import AsyncMock

from searchpipeline_events.clients import (
    PatternMatcherClient,
    QueryExecutorClient,
    QueryInterpreterClient,
    SearchGatewayClient,
)
from searchpipeline_events import ServiceName


class TestPatternMatcherClient:
    """Test PatternMatcherClient"""
    
    @pytest.mark.asyncio
    async def test_initialization(self, mock_api_url):
        """Test PatternMatcherClient initialization"""
        client = PatternMatcherClient(mock_api_url)
        
        assert client.client.api_url == mock_api_url
        assert client.client.service_name == ServiceName.PATTERN_MATCHER
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_pattern_found(self, mock_api_url, monkeypatch):
        """Test pattern_found method"""
        client = PatternMatcherClient(mock_api_url)
        
        # Mock the underlying send_pattern_match method
        mock_send = AsyncMock(return_value=True)
        monkeypatch.setattr(client.client, 'send_pattern_match', mock_send)
        
        result = await client.pattern_found(
            query="Apple stock price",
            pattern="financial_data",
            confidence=0.95,
            match_type="exact",
            processing_time_ms=45
        )
        
        assert result is True
        mock_send.assert_called_once_with(
            query="Apple stock price",
            pattern="financial_data",
            confidence=0.95,
            match_type="exact",
            processing_time_ms=45
        )
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_pattern_not_found(self, mock_api_url, monkeypatch):
        """Test pattern_not_found method"""
        client = PatternMatcherClient(mock_api_url)
        
        # Mock the underlying send_pattern_no_match method
        mock_send = AsyncMock(return_value=True)
        monkeypatch.setattr(client.client, 'send_pattern_no_match', mock_send)
        
        result = await client.pattern_not_found(
            query="random query",
            processing_time_ms=30
        )
        
        assert result is True
        mock_send.assert_called_once_with(
            query="random query",
            processing_time_ms=30
        )
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_context_manager(self, mock_api_url, monkeypatch):
        """Test async context manager"""
        mock_send = AsyncMock(return_value=True)
        
        async with PatternMatcherClient(mock_api_url) as client:
            monkeypatch.setattr(client.client, 'send_pattern_match', mock_send)
            
            await client.pattern_found(
                query="test",
                pattern="test",
                confidence=0.5,
                match_type="exact",
                processing_time_ms=10
            )
            
            assert mock_send.called


class TestQueryExecutorClient:
    """Test QueryExecutorClient"""
    
    @pytest.mark.asyncio
    async def test_initialization(self, mock_api_url):
        """Test QueryExecutorClient initialization"""
        client = QueryExecutorClient(mock_api_url)
        
        assert client.client.api_url == mock_api_url
        assert client.client.service_name == ServiceName.QUERY_EXECUTOR
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_query_executed(self, mock_api_url, monkeypatch):
        """Test query_executed method"""
        client = QueryExecutorClient(mock_api_url)
        
        # Mock the underlying send_query_execution method
        mock_send = AsyncMock(return_value=True)
        monkeypatch.setattr(client.client, 'send_query_execution', mock_send)
        
        result = await client.query_executed(
            query="SELECT * FROM stocks",
            results_count=42,
            execution_time_ms=150,
            data_source="zilliz",
            filters_applied=["date_range"]
        )
        
        assert result is True
        mock_send.assert_called_once_with(
            query="SELECT * FROM stocks",
            results_count=42,
            execution_time_ms=150,
            data_source="zilliz",
            filters_applied=["date_range"]
        )
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_query_failed(self, mock_api_url, monkeypatch):
        """Test query_failed method"""
        client = QueryExecutorClient(mock_api_url)
        
        # Mock the underlying send_query_error method
        mock_send = AsyncMock(return_value=True)
        monkeypatch.setattr(client.client, 'send_query_error', mock_send)
        
        result = await client.query_failed(
            query="SELECT * FROM invalid",
            error_type="validation",
            error_message="Table not found",
            execution_time_ms=25
        )
        
        assert result is True
        mock_send.assert_called_once_with(
            query="SELECT * FROM invalid",
            error_type="validation",
            error_message="Table not found",
            execution_time_ms=25
        )
        
        await client.close()


class TestQueryInterpreterClient:
    """Test QueryInterpreterClient"""
    
    @pytest.mark.asyncio
    async def test_initialization(self, mock_api_url):
        """Test QueryInterpreterClient initialization"""
        client = QueryInterpreterClient(mock_api_url)
        
        assert client.client.api_url == mock_api_url
        assert client.client.service_name == ServiceName.QUERY_INTERPRETER
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_query_interpreted(self, mock_api_url, monkeypatch):
        """Test query_interpreted method"""
        client = QueryInterpreterClient(mock_api_url)
        
        # Mock the underlying send_event method
        mock_send = AsyncMock(return_value=True)
        monkeypatch.setattr(client.client, 'send_event', mock_send)
        
        result = await client.query_interpreted(
            original_query="show me Apple stock data",
            interpreted_query="SELECT * FROM stocks WHERE symbol = 'AAPL'",
            interpretation_confidence=0.92,
            processing_time_ms=200
        )
        
        assert result is True
        mock_send.assert_called_once()
        
        # Check the event that was sent
        event = mock_send.call_args[0][0]
        assert event.event.value == "query_interpretation"
        assert event.service == ServiceName.QUERY_INTERPRETER
        assert event.data.original_query == "show me Apple stock data"
        assert event.data.interpreted_query == "SELECT * FROM stocks WHERE symbol = 'AAPL'"
        
        await client.close()


class TestSearchGatewayClient:
    """Test SearchGatewayClient"""
    
    @pytest.mark.asyncio
    async def test_initialization(self, mock_api_url):
        """Test SearchGatewayClient initialization"""
        client = SearchGatewayClient(mock_api_url)
        
        assert client.client.api_url == mock_api_url
        assert client.client.service_name == ServiceName.SEARCH_GATEWAY
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_search_requested(self, mock_api_url, monkeypatch):
        """Test search_requested method"""
        client = SearchGatewayClient(mock_api_url)
        
        # Mock the underlying send_search_request method
        mock_send = AsyncMock(return_value=True)
        monkeypatch.setattr(client.client, 'send_search_request', mock_send)
        
        result = await client.search_requested(
            query="Apple earnings",
            user_id="user123",
            session_id="session456",
            ip_address="192.168.1.1",
            user_agent="Mozilla/5.0"
        )
        
        assert result is True
        mock_send.assert_called_once_with(
            query="Apple earnings",
            user_id="user123",
            session_id="session456",
            ip_address="192.168.1.1",
            user_agent="Mozilla/5.0"
        )
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_rate_limit_hit(self, mock_api_url, monkeypatch):
        """Test rate_limit_hit method"""
        client = SearchGatewayClient(mock_api_url)
        
        # Mock the underlying send_event method
        mock_send = AsyncMock(return_value=True)
        monkeypatch.setattr(client.client, 'send_event', mock_send)
        
        result = await client.rate_limit_hit(
            user_id="user123",
            ip_address="192.168.1.1",
            limit_type="requests_per_minute",
            current_count=61,
            limit=60
        )
        
        assert result is True
        mock_send.assert_called_once()
        
        # Check the event that was sent
        event = mock_send.call_args[0][0]
        assert event.event.value == "rate_limit_hit"
        assert event.service == ServiceName.SEARCH_GATEWAY
        assert event.data.user_id == "user123"
        assert event.data.current_count == 61
        
        await client.close()


class TestClientConfiguration:
    """Test client configuration options"""
    
    @pytest.mark.asyncio
    async def test_custom_configuration(self, mock_api_url):
        """Test clients with custom configuration"""
        client = PatternMatcherClient(
            mock_api_url,
            timeout=10.0,
            max_retries=5,
            batch_size=20
        )
        
        assert client.client.timeout == 10.0
        assert client.client.max_retries == 5
        assert client.client.batch_size == 20
        
        await client.close()


class TestErrorHandling:
    """Test error handling in service clients"""
    
    @pytest.mark.asyncio
    async def test_pattern_matcher_error_handling(self, mock_api_url, monkeypatch):
        """Test error handling in PatternMatcherClient"""
        client = PatternMatcherClient(mock_api_url)
        
        # Mock the underlying method to raise an exception
        mock_send = AsyncMock(side_effect=Exception("Network error"))
        monkeypatch.setattr(client.client, 'send_pattern_match', mock_send)
        
        # Should propagate the exception
        with pytest.raises(Exception, match="Network error"):
            await client.pattern_found(
                query="test",
                pattern="test",
                confidence=0.5,
                match_type="exact",
                processing_time_ms=10
            )
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_query_executor_error_handling(self, mock_api_url, monkeypatch):
        """Test error handling in QueryExecutorClient"""
        client = QueryExecutorClient(mock_api_url)
        
        # Mock the underlying method to return False (failure)
        mock_send = AsyncMock(return_value=False)
        monkeypatch.setattr(client.client, 'send_query_execution', mock_send)
        
        result = await client.query_executed(
            query="test",
            results_count=0,
            execution_time_ms=100,
            data_source="test"
        )
        
        assert result is False
        
        await client.close()