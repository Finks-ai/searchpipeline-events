# Search Pipeline Events

Standardized event schemas and client library for the search pipeline data collection service.

## Installation

```bash
pip install -e ../searchpipeline-events
```

## Quick Start

```python
from searchpipeline_events import EventClient, ServiceName

# Initialize client
client = EventClient(
    api_url="https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect",
    service_name=ServiceName.PATTERN_MATCHER
)

# Send events
await client.send_pattern_match(
    query="Apple stock price",
    pattern="financial_data",
    confidence=0.95,
    match_type="exact",
    processing_time_ms=45
)
```

## Event Types

- **Pattern Matcher**: `pattern_match`, `pattern_no_match`
- **Query Executor**: `query_execution`, `query_error`
- **Query Interpreter**: `query_interpretation`
- **Search Gateway**: `search_request`, `rate_limit_hit`
- **Generic**: `service_start`, `service_stop`, `error`

## Service-Specific Clients

```python
from searchpipeline_events.clients import PatternMatcherClient, QueryExecutorClient

# Pattern matcher
async with PatternMatcherClient(api_url) as client:
    await client.pattern_found(query, pattern, confidence, match_type, processing_time_ms)
    await client.pattern_not_found(query, attempted_patterns, processing_time_ms)

# Query executor
async with QueryExecutorClient(api_url) as client:
    await client.query_executed(query, results_count, execution_time_ms, data_source)
    await client.query_failed(query, error_type, error_message, execution_time_ms)
```

## Decorators

```python
from searchpipeline_events.decorators import track_query_execution

@track_query_execution(
    data_source="zilliz",
    extract_query=lambda args, kwargs, result: args[0],
    extract_results_count=lambda result: len(result)
)
async def search_data(query: str):
    # Your search logic here
    return results
```

## Configuration

Set environment variables or pass directly:

```bash
export EVENTS_API_URL="https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect"
export SERVICE_NAME="pattern-matcher"
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/
ruff src/
```