"""
Tests for event tracking decorators.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from searchpipeline_events.decorators import (
    track_execution,
    track_query_execution,
    track_pattern_matching,
    get_query_from_first_arg,
    get_results_count_from_list,
    get_pattern_info_from_result,
)
from searchpipeline_events import EventClient, ServiceName


class TestTrackingDecorators:
    """Test tracking decorators"""
    
    @pytest.mark.asyncio
    async def test_track_execution_async_success(self, mock_api_url, monkeypatch):
        """Test track_execution decorator with async function success"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_query_execution = AsyncMock(return_value=True)
        
        @track_execution(
            event_type='query_execution',
            client=mock_client,
            extract_query=lambda args, kwargs, result: args[0],
            extract_results_count=lambda result: len(result),
            extract_context=lambda args, kwargs, result: {"data_source": "test"}
        )
        async def test_function(query: str):
            await asyncio.sleep(0.01)  # Simulate work
            return [{"id": 1}, {"id": 2}]
        
        result = await test_function("SELECT * FROM test")
        
        assert result == [{"id": 1}, {"id": 2}]
        mock_client.send_query_execution.assert_called_once()
        
        # Check the call arguments
        call_args = mock_client.send_query_execution.call_args
        assert call_args[1]['query'] == "SELECT * FROM test"
        assert call_args[1]['results_count'] == 2
        assert call_args[1]['data_source'] == "test"
        assert call_args[1]['execution_time_ms'] > 0
    
    @pytest.mark.asyncio
    async def test_track_execution_async_error(self, mock_api_url, monkeypatch):
        """Test track_execution decorator with async function error"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_error = AsyncMock(return_value=True)
        
        @track_execution(
            event_type='query_execution',
            client=mock_client,
            track_errors=True
        )
        async def test_function(query: str):
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            await test_function("SELECT * FROM test")
        
        mock_client.send_error.assert_called_once()
        
        # Check error event details
        call_args = mock_client.send_error.call_args
        assert call_args[1]['error_type'] == "ValueError"
        assert call_args[1]['error_message'] == "Test error"
        assert "test_function" in call_args[1]['context']['function']
    
    def test_track_execution_sync_success(self, mock_api_url, monkeypatch):
        """Test track_execution decorator with sync function success"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_query_execution = AsyncMock(return_value=True)
        
        # Mock asyncio.get_event_loop and create_task
        mock_loop = MagicMock()
        mock_task = MagicMock()
        mock_loop.create_task.return_value = mock_task
        
        with patch('asyncio.get_event_loop', return_value=mock_loop):
            @track_execution(
                event_type='query_execution',
                client=mock_client,
                extract_query=lambda args, kwargs, result: args[0],
                extract_results_count=lambda result: len(result),
                extract_context=lambda args, kwargs, result: {"data_source": "test"}
            )
            def test_function(query: str):
                return [{"id": 1}, {"id": 2}]
            
            result = test_function("SELECT * FROM test")
            
            assert result == [{"id": 1}, {"id": 2}]
            mock_loop.create_task.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_track_query_execution_decorator(self, mock_api_url, monkeypatch):
        """Test track_query_execution specific decorator"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_query_execution = AsyncMock(return_value=True)
        
        @track_query_execution(
            data_source="zilliz",
            client=mock_client,
            extract_query=get_query_from_first_arg,
            extract_results_count=get_results_count_from_list
        )
        async def search_data(query: str):
            return [{"result": 1}, {"result": 2}, {"result": 3}]
        
        result = await search_data("test query")
        
        assert len(result) == 3
        mock_client.send_query_execution.assert_called_once()
        
        call_args = mock_client.send_query_execution.call_args
        assert call_args[1]['query'] == "test query"
        assert call_args[1]['results_count'] == 3
        assert call_args[1]['data_source'] == "zilliz"
    
    @pytest.mark.asyncio
    async def test_track_pattern_matching_decorator(self, mock_api_url, monkeypatch):
        """Test track_pattern_matching specific decorator"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_pattern_match = AsyncMock(return_value=True)
        
        @track_pattern_matching(
            client=mock_client,
            extract_query=get_query_from_first_arg,
            extract_pattern_info=get_pattern_info_from_result
        )
        async def match_pattern(query: str):
            return {
                "pattern": "financial_data",
                "confidence": 0.95,
                "match_type": "exact"
            }
        
        result = await match_pattern("Apple stock price")
        
        assert result["pattern"] == "financial_data"
        mock_client.send_pattern_match.assert_called_once()
        
        call_args = mock_client.send_pattern_match.call_args
        assert call_args[1]['query'] == "Apple stock price"
        assert call_args[1]['pattern'] == "financial_data"
        assert call_args[1]['confidence'] == 0.95
        assert call_args[1]['match_type'] == "exact"
    
    @pytest.mark.asyncio
    async def test_decorator_no_client(self):
        """Test decorator behavior when no client is available"""
        @track_execution(event_type='query_execution')
        async def test_function():
            return "success"
        
        # Should work without error even without client
        result = await test_function()
        assert result == "success"
    
    @pytest.mark.asyncio
    async def test_decorator_with_global_client(self, mock_api_url, monkeypatch):
        """Test decorator using global client"""
        from searchpipeline_events.client import init_global_client
        
        # Initialize global client
        global_client = init_global_client(mock_api_url, ServiceName.PATTERN_MATCHER)
        global_client.send_query_execution = AsyncMock(return_value=True)
        
        @track_query_execution(
            data_source="test",
            extract_query=get_query_from_first_arg,
            extract_results_count=get_results_count_from_list
        )
        async def test_function(query: str):
            return ["result1", "result2"]
        
        result = await test_function("test query")
        
        assert len(result) == 2
        global_client.send_query_execution.assert_called_once()
        
        await global_client.close()


class TestHelperFunctions:
    """Test helper functions for extracting data"""
    
    def test_get_query_from_first_arg(self):
        """Test query extraction from first argument"""
        args = ("SELECT * FROM table", "other_arg")
        kwargs = {"param": "value"}
        result = None
        
        query = get_query_from_first_arg(args, kwargs, result)
        assert query == "SELECT * FROM table"
    
    def test_get_query_from_first_arg_empty(self):
        """Test query extraction with empty args"""
        args = ()
        kwargs = {"param": "value"}
        result = None
        
        query = get_query_from_first_arg(args, kwargs, result)
        assert query == ""
    
    def test_get_results_count_from_list(self):
        """Test results count extraction from list"""
        result = [{"id": 1}, {"id": 2}, {"id": 3}]
        
        count = get_results_count_from_list(result)
        assert count == 3
    
    def test_get_results_count_from_dict_with_results(self):
        """Test results count extraction from dict with results key"""
        result = {"results": [{"id": 1}, {"id": 2}], "total": 2}
        
        count = get_results_count_from_list(result)
        assert count == 2
    
    def test_get_results_count_from_other(self):
        """Test results count extraction from other types"""
        result = "not a list or dict"
        
        count = get_results_count_from_list(result)
        assert count == 0
    
    def test_get_pattern_info_from_result_dict(self):
        """Test pattern info extraction from dict result"""
        result = {
            "pattern": "financial_data",
            "confidence": 0.95,
            "match_type": "exact",
            "other_field": "ignored"
        }
        
        info = get_pattern_info_from_result(result)
        
        assert info["pattern"] == "financial_data"
        assert info["confidence"] == 0.95
        assert info["match_type"] == "exact"
        assert "other_field" not in info
    
    def test_get_pattern_info_from_result_partial(self):
        """Test pattern info extraction from partial dict"""
        result = {
            "pattern": "financial_data",
            "confidence": 0.95
            # missing match_type
        }
        
        info = get_pattern_info_from_result(result)
        
        assert info["pattern"] == "financial_data"
        assert info["confidence"] == 0.95
        assert info["match_type"] == "unknown"  # default value
    
    def test_get_pattern_info_from_result_non_dict(self):
        """Test pattern info extraction from non-dict result"""
        result = "not a dict"
        
        info = get_pattern_info_from_result(result)
        
        assert info == {}


class TestDecoratorErrorHandling:
    """Test error handling in decorators"""
    
    @pytest.mark.asyncio
    async def test_decorator_extract_function_error(self, mock_api_url, monkeypatch):
        """Test decorator when extract function raises error"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_query_execution = AsyncMock(return_value=True)
        
        def failing_extract(args, kwargs, result):
            raise ValueError("Extract failed")
        
        @track_execution(
            event_type='query_execution',
            client=mock_client,
            extract_query=failing_extract,
            extract_results_count=get_results_count_from_list,
            extract_context=lambda args, kwargs, result: {"data_source": "test"}
        )
        async def test_function():
            return ["result"]
        
        result = await test_function()
        
        assert result == ["result"]
        mock_client.send_query_execution.assert_called_once()
        
        # Should use "unknown" when extract function fails
        call_args = mock_client.send_query_execution.call_args
        assert call_args[1]['query'] == "unknown"
    
    @pytest.mark.asyncio
    async def test_decorator_send_event_error(self, mock_api_url, monkeypatch):
        """Test decorator when sending event fails"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_query_execution = AsyncMock(side_effect=Exception("Send failed"))
        
        @track_execution(
            event_type='query_execution',
            client=mock_client,
            extract_query=get_query_from_first_arg,
            extract_results_count=get_results_count_from_list
        )
        async def test_function(query: str):
            return ["result"]
        
        # Should not raise error even if event sending fails
        result = await test_function("test query")
        
        assert result == ["result"]
        mock_client.send_query_execution.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_decorator_no_error_tracking(self, mock_api_url, monkeypatch):
        """Test decorator with error tracking disabled"""
        mock_client = AsyncMock(spec=EventClient)
        mock_client.send_error = AsyncMock(return_value=True)
        
        @track_execution(
            event_type='query_execution',
            client=mock_client,
            track_errors=False
        )
        async def test_function():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            await test_function()
        
        # Should not send error event when track_errors=False
        mock_client.send_error.assert_not_called()