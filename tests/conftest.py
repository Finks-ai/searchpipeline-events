"""
Shared test fixtures and configuration for searchpipeline-events tests.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
import httpx

from searchpipeline_events import EventClient, ServiceName


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_api_url():
    """Mock API URL for testing"""
    return "https://test-api.example.com/collect"


@pytest.fixture
def mock_httpx_client():
    """Mock httpx AsyncClient"""
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response
    return mock_client


@pytest.fixture
async def event_client(mock_api_url, mock_httpx_client, monkeypatch):
    """Create EventClient with mocked HTTP client"""
    client = EventClient(mock_api_url, ServiceName.PATTERN_MATCHER)
    
    # Replace the real httpx client with our mock
    monkeypatch.setattr(client, 'client', mock_httpx_client)
    
    yield client
    
    # Clean up
    await client.close()


@pytest.fixture
def sample_pattern_match_data():
    """Sample pattern match event data"""
    return {
        "query": "Apple stock price",
        "pattern": "financial_data",
        "confidence": 0.95,
        "match_type": "exact",
        "processing_time_ms": 45
    }


@pytest.fixture
def sample_query_execution_data():
    """Sample query execution event data"""
    return {
        "query": "SELECT * FROM stocks WHERE symbol = 'AAPL'",
        "results_count": 42,
        "execution_time_ms": 150,
        "data_source": "zilliz",
        "filters_applied": ["date_range", "sector"]
    }


@pytest.fixture
def sample_search_request_data():
    """Sample search request event data"""
    return {
        "query": "Apple earnings report",
        "user_id": "user123",
        "session_id": "session456",
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0 (Test Browser)"
    }


@pytest.fixture
def sample_error_data():
    """Sample error event data"""
    return {
        "error_type": "ValidationError",
        "error_message": "Invalid input format",
        "stack_trace": "Traceback...",
        "context": {"field": "query", "value": "invalid"}
    }