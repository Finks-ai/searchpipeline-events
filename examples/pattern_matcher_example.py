"""
Example: Pattern Matcher Service Integration
"""

import asyncio
from searchpipeline_events import PatternMatcherClient, track_pattern_matching, get_pattern_info_from_result


# Example 1: Using the service-specific client
async def example_service_client():
    """Example using PatternMatcherClient"""
    
    api_url = "https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect"
    
    async with PatternMatcherClient(api_url) as client:
        # Pattern found
        await client.pattern_found(
            query="Apple stock price today",
            pattern="financial_data",
            confidence=0.95,
            match_type="exact",
            processing_time_ms=45
        )
        
        # Pattern not found
        await client.pattern_not_found(
            query="random query",
            attempted_patterns=["financial_data", "company_info", "market_data"],
            processing_time_ms=30
        )


# Example 2: Using decorators for automatic tracking
@track_pattern_matching(
    extract_query=lambda args, kwargs, result: args[0],
    extract_pattern_info=get_pattern_info_from_result
)
async def match_pattern(query: str, patterns: list):
    """Example pattern matching function with automatic event tracking"""
    
    # Your pattern matching logic here
    for pattern in patterns:
        if "stock" in query.lower() and pattern == "financial_data":
            return {
                "pattern": pattern,
                "confidence": 0.95,
                "match_type": "exact",
                "matched": True
            }
    
    return {
        "pattern": None,
        "confidence": 0.0,
        "match_type": "none",
        "matched": False
    }


# Example 3: Manual event creation
async def example_manual_events():
    """Example of manual event creation"""
    
    from searchpipeline_events import EventClient, ServiceName
    
    client = EventClient(
        api_url="https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect",
        service_name=ServiceName.PATTERN_MATCHER
    )
    
    # Manual pattern match event
    await client.send_pattern_match(
        query="Tesla earnings report",
        pattern="financial_data",
        confidence=0.88,
        match_type="semantic",
        processing_time_ms=67
    )
    
    # Manual error event
    await client.send_error(
        error_type="ValidationError",
        error_message="Invalid pattern configuration",
        context={"pattern": "invalid_pattern"}
    )
    
    await client.close()


# Example 4: Integration with existing pattern matcher
class PatternMatcher:
    """Example pattern matcher class"""
    
    def __init__(self, api_url: str):
        self.event_client = PatternMatcherClient(api_url)
        self.patterns = {
            "financial_data": ["stock", "price", "earnings", "revenue"],
            "company_info": ["company", "ceo", "founded", "employees"],
            "market_data": ["market", "index", "dow", "nasdaq"]
        }
    
    async def match(self, query: str):
        """Match query against patterns"""
        start_time = asyncio.get_event_loop().time()
        
        query_lower = query.lower()
        best_match = None
        best_confidence = 0.0
        
        for pattern_name, keywords in self.patterns.items():
            matches = sum(1 for keyword in keywords if keyword in query_lower)
            confidence = matches / len(keywords)
            
            if confidence > best_confidence:
                best_confidence = confidence
                best_match = pattern_name
        
        processing_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
        
        if best_match and best_confidence > 0.5:
            # Pattern found
            await self.event_client.pattern_found(
                query=query,
                pattern=best_match,
                confidence=best_confidence,
                match_type="fuzzy",
                processing_time_ms=processing_time_ms
            )
            
            return {
                "pattern": best_match,
                "confidence": best_confidence,
                "matched": True
            }
        else:
            # No pattern found
            await self.event_client.pattern_not_found(
                query=query,
                attempted_patterns=list(self.patterns.keys()),
                processing_time_ms=processing_time_ms
            )
            
            return {
                "pattern": None,
                "confidence": 0.0,
                "matched": False
            }
    
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
        service_name=ServiceName.PATTERN_MATCHER
    )
    
    result = await match_pattern("Apple stock price", ["financial_data", "company_info"])
    print(f"Pattern match result: {result}")
    
    print("\nExample 3: Manual event creation")
    await example_manual_events()
    
    print("\nExample 4: Integrated pattern matcher")
    matcher = PatternMatcher("https://api-gateway-id.execute-api.us-east-1.amazonaws.com/dev/collect")
    
    # Test successful match
    result = await matcher.match("What is Apple's stock price?")
    print(f"Match result: {result}")
    
    # Test failed match
    result = await matcher.match("What's the weather like?")
    print(f"No match result: {result}")
    
    await matcher.close()


if __name__ == "__main__":
    asyncio.run(main())