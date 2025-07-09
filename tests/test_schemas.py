"""
Tests for event schemas and validation.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from searchpipeline_events.schemas import (
    BaseEvent,
    EventType,
    ServiceName,
    PatternMatchData,
    PatternNoMatchData,
    QueryExecutionData,
    QueryErrorData,
    SearchRequestData,
    ErrorData,
    create_pattern_match_event,
    create_query_execution_event,
    create_search_request_event,
    create_error_event,
)


class TestEventSchemas:
    """Test event schema validation"""
    
    def test_pattern_match_data_valid(self):
        """Test valid pattern match data creation"""
        data = PatternMatchData(
            query="Apple stock price",
            pattern="financial_data",
            confidence=0.95,
            match_type="exact"
        )
        
        assert data.query == "Apple stock price"
        assert data.pattern == "financial_data"
        assert data.confidence == 0.95
        assert data.match_type == "exact"
        assert isinstance(data.timestamp, datetime)
    
    def test_pattern_match_data_invalid_confidence(self):
        """Test pattern match data with invalid confidence"""
        with pytest.raises(ValidationError):
            PatternMatchData(
                query="test",
                pattern="test",
                confidence=1.5,  # Invalid: > 1.0
                match_type="exact"
            )
    
    def test_pattern_match_data_invalid_match_type(self):
        """Test pattern match data with invalid match type"""
        with pytest.raises(ValidationError):
            PatternMatchData(
                query="test",
                pattern="test",
                confidence=0.5,
                match_type="invalid_type"  # Invalid match type
            )
    
    def test_query_execution_data_valid(self):
        """Test valid query execution data creation"""
        data = QueryExecutionData(
            query="SELECT * FROM stocks",
            results_count=42,
            execution_time_ms=150,
            data_source="zilliz",
            filters_applied=["date_range"]
        )
        
        assert data.query == "SELECT * FROM stocks"
        assert data.results_count == 42
        assert data.execution_time_ms == 150
        assert data.data_source == "zilliz"
        assert data.filters_applied == ["date_range"]
    
    def test_query_execution_data_negative_results(self):
        """Test query execution data with negative results count"""
        with pytest.raises(ValidationError):
            QueryExecutionData(
                query="test",
                results_count=-1,  # Invalid: negative
                execution_time_ms=100,
                data_source="test"
            )
    
    def test_query_error_data_valid(self):
        """Test valid query error data creation"""
        data = QueryErrorData(
            query="SELECT * FROM invalid",
            error_type="validation",
            error_message="Table not found",
            execution_time_ms=50
        )
        
        assert data.query == "SELECT * FROM invalid"
        assert data.error_type == "validation"
        assert data.error_message == "Table not found"
        assert data.execution_time_ms == 50
    
    def test_query_error_data_invalid_error_type(self):
        """Test query error data with invalid error type"""
        with pytest.raises(ValidationError):
            QueryErrorData(
                query="test",
                error_type="invalid_error_type",  # Invalid type
                error_message="test",
                execution_time_ms=50
            )
    
    def test_search_request_data_valid(self):
        """Test valid search request data creation"""
        data = SearchRequestData(
            query="Apple earnings",
            user_id="user123",
            session_id="session456",
            ip_address="192.168.1.1"
        )
        
        assert data.query == "Apple earnings"
        assert data.user_id == "user123"
        assert data.session_id == "session456"
        assert data.ip_address == "192.168.1.1"
    
    def test_error_data_valid(self):
        """Test valid error data creation"""
        data = ErrorData(
            error_type="ValueError",
            error_message="Invalid input",
            stack_trace="Traceback...",
            context={"field": "query"}
        )
        
        assert data.error_type == "ValueError"
        assert data.error_message == "Invalid input"
        assert data.stack_trace == "Traceback..."
        assert data.context == {"field": "query"}


class TestBaseEvent:
    """Test base event structure"""
    
    def test_base_event_valid(self):
        """Test valid base event creation"""
        event = BaseEvent(
            event=EventType.PATTERN_MATCH,
            service=ServiceName.PATTERN_MATCHER,
            data=PatternMatchData(
                query="test",
                pattern="test",
                confidence=0.5,
                match_type="exact"
            )
        )
        
        assert event.event == EventType.PATTERN_MATCH
        assert event.service == ServiceName.PATTERN_MATCHER
        assert isinstance(event.data, PatternMatchData)
    
    def test_base_event_with_dict_data(self):
        """Test base event with dictionary data"""
        event = BaseEvent(
            event=EventType.PATTERN_MATCH,
            service=ServiceName.PATTERN_MATCHER,
            data={
                "query": "test",
                "pattern": "test",
                "confidence": 0.5,
                "match_type": "exact"
            }
        )
        
        assert event.event == EventType.PATTERN_MATCH
        assert event.service == ServiceName.PATTERN_MATCHER
        # Should convert dict to PatternMatchData
        assert isinstance(event.data, PatternMatchData)


class TestEventCreationFunctions:
    """Test event creation helper functions"""
    
    def test_create_pattern_match_event(self):
        """Test pattern match event creation"""
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            query="Apple stock",
            pattern="financial_data",
            confidence=0.95,
            match_type="exact",
            processing_time_ms=45
        )
        
        assert event.event == EventType.PATTERN_MATCH
        assert event.service == ServiceName.PATTERN_MATCHER
        assert event.data.query == "Apple stock"
        assert event.data.pattern == "financial_data"
        assert event.data.confidence == 0.95
        assert event.data.processing_time_ms == 45
    
    def test_create_query_execution_event(self):
        """Test query execution event creation"""
        event = create_query_execution_event(
            service=ServiceName.QUERY_EXECUTOR,
            query="SELECT * FROM stocks",
            results_count=42,
            execution_time_ms=150,
            data_source="zilliz",
            filters_applied=["date_range"]
        )
        
        assert event.event == EventType.QUERY_EXECUTION
        assert event.service == ServiceName.QUERY_EXECUTOR
        assert event.data.query == "SELECT * FROM stocks"
        assert event.data.results_count == 42
        assert event.data.execution_time_ms == 150
        assert event.data.data_source == "zilliz"
        assert event.data.filters_applied == ["date_range"]
    
    def test_create_search_request_event(self):
        """Test search request event creation"""
        event = create_search_request_event(
            service=ServiceName.SEARCH_GATEWAY,
            query="Apple earnings",
            user_id="user123",
            session_id="session456"
        )
        
        assert event.event == EventType.SEARCH_REQUEST
        assert event.service == ServiceName.SEARCH_GATEWAY
        assert event.data.query == "Apple earnings"
        assert event.data.user_id == "user123"
        assert event.data.session_id == "session456"
    
    def test_create_error_event(self):
        """Test error event creation"""
        event = create_error_event(
            service=ServiceName.PATTERN_MATCHER,
            error_type="ValueError",
            error_message="Invalid input",
            context={"field": "query"}
        )
        
        assert event.event == EventType.ERROR
        assert event.service == ServiceName.PATTERN_MATCHER
        assert event.data.error_type == "ValueError"
        assert event.data.error_message == "Invalid input"
        assert event.data.context == {"field": "query"}


class TestEnums:
    """Test enum values"""
    
    def test_event_types(self):
        """Test all event types are defined"""
        expected_events = [
            "pattern_match",
            "pattern_no_match", 
            "query_execution",
            "query_error",
            "query_interpretation",
            "search_request",
            "rate_limit_hit",
            "service_start",
            "service_stop",
            "error"
        ]
        
        for event in expected_events:
            assert event in [e.value for e in EventType]
    
    def test_service_names(self):
        """Test all service names are defined"""
        expected_services = [
            "search-gateway",
            "pattern-matcher",
            "query-interpreter",
            "query-executor",
            "data-collection",
            "etl-pipeline"
        ]
        
        for service in expected_services:
            assert service in [s.value for s in ServiceName]


class TestJSONSerialization:
    """Test JSON serialization of events"""
    
    def test_event_json_serialization(self):
        """Test that events can be serialized to JSON"""
        event = create_pattern_match_event(
            service=ServiceName.PATTERN_MATCHER,
            query="test query",
            pattern="test_pattern",
            confidence=0.75,
            match_type="fuzzy"
        )
        
        # Should be able to convert to dict for JSON serialization
        event_dict = event.model_dump()
        
        assert event_dict["event"] == "pattern_match"
        assert event_dict["service"] == "pattern-matcher"
        assert event_dict["data"]["query"] == "test query"
        assert event_dict["data"]["pattern"] == "test_pattern"
        assert event_dict["data"]["confidence"] == 0.75
        assert "timestamp" in event_dict["data"]
    
    def test_datetime_serialization(self):
        """Test datetime fields are properly serialized"""
        data = PatternMatchData(
            query="test",
            pattern="test",
            confidence=0.5,
            match_type="exact"
        )
        
        data_dict = data.model_dump()
        
        # Timestamp should be serialized as ISO string or datetime object
        timestamp = data_dict["timestamp"]
        if isinstance(timestamp, str):
            assert "T" in timestamp  # ISO format indicator
        else:
            # In Pydantic v2, datetime objects might be preserved
            assert isinstance(timestamp, datetime)