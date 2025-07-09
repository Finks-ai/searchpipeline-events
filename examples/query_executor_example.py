"""
Example: Query Executor Service Integration
"""

import asyncio
from searchpipeline_events import QueryExecutorClient, track_query_execution


# Example 1: Using the service-specific client
async def example_service_client():
    """Example using QueryExecutorClient"""
    
    api_url = "https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect"
    
    async with QueryExecutorClient(api_url) as client:
        # Successful query execution
        await client.query_executed(
            query="SELECT * FROM stocks WHERE symbol = 'AAPL'",
            results_count=42,
            execution_time_ms=150,
            data_source="zilliz",
            filters_applied=["date_range", "sector"]
        )
        
        # Failed query execution
        await client.query_failed(
            query="SELECT * FROM invalid_table",
            error_type="validation",
            error_message="Table 'invalid_table' does not exist",
            execution_time_ms=25
        )


# Example 2: Using decorators for automatic tracking
@track_query_execution(
    data_source="zilliz",
    extract_query=lambda args, kwargs, result: args[0],
    extract_results_count=lambda result: len(result) if isinstance(result, list) else 0
)
async def execute_query(query: str, filters: dict = None):
    """Example query execution function with automatic event tracking"""
    
    # Simulate database query
    await asyncio.sleep(0.1)  # Simulate execution time
    
    # Mock results based on query
    if "AAPL" in query:
        return [
            {"symbol": "AAPL", "price": 150.25, "volume": 1000000},
            {"symbol": "AAPL", "price": 149.80, "volume": 950000},
        ]
    elif "error" in query.lower():
        raise ValueError("Simulated query error")
    else:
        return []


# Example 3: Integration with existing query executor
class QueryExecutor:
    """Example query executor class"""
    
    def __init__(self, api_url: str):
        self.event_client = QueryExecutorClient(api_url)
        self.data_source = "zilliz"
        
        # Mock data
        self.mock_data = {
            "AAPL": [
                {"symbol": "AAPL", "price": 150.25, "timestamp": "2024-01-09T10:00:00Z"},
                {"symbol": "AAPL", "price": 149.80, "timestamp": "2024-01-09T09:30:00Z"},
            ],
            "GOOGL": [
                {"symbol": "GOOGL", "price": 2800.50, "timestamp": "2024-01-09T10:00:00Z"},
            ]
        }
    
    async def execute(self, query: str, filters: dict = None):
        """Execute query and track performance"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Parse query (simplified)
            symbol = None
            if "AAPL" in query:
                symbol = "AAPL"
            elif "GOOGL" in query:
                symbol = "GOOGL"
            
            # Simulate execution time
            await asyncio.sleep(0.1)
            
            # Get results
            results = self.mock_data.get(symbol, [])
            
            # Apply filters if provided
            if filters:
                # Simulate filter application
                if "limit" in filters:
                    results = results[:filters["limit"]]
            
            execution_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
            
            # Track successful execution
            await self.event_client.query_executed(
                query=query,
                results_count=len(results),
                execution_time_ms=execution_time_ms,
                data_source=self.data_source,
                filters_applied=list(filters.keys()) if filters else None
            )
            
            return {
                "results": results,
                "count": len(results),
                "execution_time_ms": execution_time_ms
            }
            
        except Exception as e:
            execution_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
            
            # Track failed execution
            await self.event_client.query_failed(
                query=query,
                error_type=type(e).__name__,
                error_message=str(e),
                execution_time_ms=execution_time_ms
            )
            
            raise
    
    async def close(self):
        """Close the event client"""
        await self.event_client.close()


# Example 4: Batch query execution with tracking
class BatchQueryExecutor:
    """Example batch query executor with event tracking"""
    
    def __init__(self, api_url: str):
        self.event_client = QueryExecutorClient(api_url)
    
    async def execute_batch(self, queries: list, batch_size: int = 10):
        """Execute multiple queries and track each one"""
        
        results = []
        
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            batch_results = []
            
            for query in batch:
                try:
                    start_time = asyncio.get_event_loop().time()
                    
                    # Simulate query execution
                    await asyncio.sleep(0.05)
                    mock_results = [{"id": 1, "data": "result"}] if "valid" in query else []
                    
                    execution_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
                    
                    await self.event_client.query_executed(
                        query=query,
                        results_count=len(mock_results),
                        execution_time_ms=execution_time_ms,
                        data_source="batch_processor"
                    )
                    
                    batch_results.append({
                        "query": query,
                        "results": mock_results,
                        "success": True
                    })
                    
                except Exception as e:
                    execution_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
                    
                    await self.event_client.query_failed(
                        query=query,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        execution_time_ms=execution_time_ms
                    )
                    
                    batch_results.append({
                        "query": query,
                        "error": str(e),
                        "success": False
                    })
            
            results.extend(batch_results)
        
        return results
    
    async def close(self):
        """Close the event client"""
        await self.event_client.close()


async def main():
    """Run all examples"""
    
    print("Example 1: Service-specific client")
    await example_service_client()
    
    print("\nExample 2: Decorator-based tracking")
    # Initialize global client for decorators
    from searchpipeline_events import init_global_client, ServiceName
    init_global_client(
        api_url="https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect",
        service_name=ServiceName.QUERY_EXECUTOR
    )
    
    # Test successful query
    result = await execute_query("SELECT * FROM stocks WHERE symbol = 'AAPL'")
    print(f"Query result: {len(result)} records")
    
    # Test failed query
    try:
        await execute_query("SELECT * FROM stocks WHERE error = true")
    except ValueError as e:
        print(f"Query failed as expected: {e}")
    
    print("\nExample 3: Integrated query executor")
    executor = QueryExecutor("https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect")
    
    # Test successful execution
    result = await executor.execute("SELECT * FROM stocks WHERE symbol = 'AAPL'")
    print(f"Execution result: {result['count']} records in {result['execution_time_ms']}ms")
    
    # Test with filters
    result = await executor.execute("SELECT * FROM stocks WHERE symbol = 'AAPL'", {"limit": 1})
    print(f"Filtered result: {result['count']} records")
    
    # Test failed execution
    try:
        await executor.execute("SELECT * FROM invalid_table")
    except KeyError:
        print("Query failed as expected")
    
    await executor.close()
    
    print("\nExample 4: Batch query execution")
    batch_executor = BatchQueryExecutor("https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect")
    
    queries = [
        "SELECT * FROM valid_table",
        "SELECT * FROM another_valid_table",
        "SELECT * FROM invalid_table"  # This will "fail"
    ]
    
    batch_results = await batch_executor.execute_batch(queries)
    successful = sum(1 for r in batch_results if r['success'])
    print(f"Batch execution: {successful}/{len(batch_results)} queries succeeded")
    
    await batch_executor.close()


if __name__ == "__main__":
    asyncio.run(main())