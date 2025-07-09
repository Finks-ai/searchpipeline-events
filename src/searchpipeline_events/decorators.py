"""
Event decorators for automatic event tracking.
"""

import asyncio
import functools
import time
import traceback
from typing import Any, Callable, Dict, List, Optional

from .client import EventClient, get_global_client
from .schemas import ServiceName


def track_execution(
    event_type: str,
    service_name: Optional[ServiceName] = None,
    client: Optional[EventClient] = None,
    track_errors: bool = True,
    extract_query: Optional[Callable[[Any], str]] = None,
    extract_results_count: Optional[Callable[[Any], int]] = None,
    extract_context: Optional[Callable[[Any], Dict[str, Any]]] = None
):
    """
    Decorator to automatically track function execution events
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            event_client = client or get_global_client()
            if not event_client:
                return await func(*args, **kwargs)
            
            start_time = time.time()
            error_occurred = False
            result = None
            
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                error_occurred = True
                
                if track_errors:
                    await event_client.send_error(
                        error_type=type(e).__name__,
                        error_message=str(e),
                        stack_trace=traceback.format_exc(),
                        context={
                            "function": func.__name__,
                            "args": str(args)[:200],
                            "kwargs": str(kwargs)[:200]
                        }
                    )
                raise
            finally:
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                if not error_occurred:
                    query = None
                    if extract_query:
                        try:
                            query = extract_query(args, kwargs, result)
                        except Exception:
                            pass
                    
                    results_count = None
                    if extract_results_count and result is not None:
                        try:
                            results_count = extract_results_count(result)
                        except Exception:
                            pass
                    
                    context = {}
                    if extract_context:
                        try:
                            context = extract_context(args, kwargs, result)
                        except Exception:
                            pass
                    
                    try:
                        if event_type == 'query_execution':
                            await event_client.send_query_execution(
                                query=query or "unknown",
                                results_count=results_count or 0,
                                execution_time_ms=execution_time_ms,
                                data_source=context.get('data_source', 'unknown')
                            )
                        elif event_type == 'pattern_match':
                            await event_client.send_pattern_match(
                                query=query or "unknown",
                                pattern=context.get('pattern', 'unknown'),
                                confidence=context.get('confidence', 0.0),
                                match_type=context.get('match_type', 'unknown'),
                                processing_time_ms=execution_time_ms
                            )
                    except Exception:
                        pass
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            event_client = client or get_global_client()
            if not event_client:
                return func(*args, **kwargs)
            
            start_time = time.time()
            error_occurred = False
            result = None
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_occurred = True
                
                if track_errors:
                    try:
                        loop = asyncio.get_event_loop()
                        loop.create_task(event_client.send_error(
                            error_type=type(e).__name__,
                            error_message=str(e),
                            stack_trace=traceback.format_exc(),
                            context={
                                "function": func.__name__,
                                "args": str(args)[:200],
                                "kwargs": str(kwargs)[:200]
                            }
                        ))
                    except Exception:
                        pass
                raise
            finally:
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                if not error_occurred:
                    query = None
                    if extract_query:
                        try:
                            query = extract_query(args, kwargs, result)
                        except Exception:
                            pass
                    
                    results_count = None
                    if extract_results_count and result is not None:
                        try:
                            results_count = extract_results_count(result)
                        except Exception:
                            pass
                    
                    context = {}
                    if extract_context:
                        try:
                            context = extract_context(args, kwargs, result)
                        except Exception:
                            pass
                    
                    try:
                        loop = asyncio.get_event_loop()
                        if event_type == 'query_execution':
                            loop.create_task(event_client.send_query_execution(
                                query=query or "unknown",
                                results_count=results_count or 0,
                                execution_time_ms=execution_time_ms,
                                data_source=context.get('data_source', 'unknown')
                            ))
                        elif event_type == 'pattern_match':
                            loop.create_task(event_client.send_pattern_match(
                                query=query or "unknown",
                                pattern=context.get('pattern', 'unknown'),
                                confidence=context.get('confidence', 0.0),
                                match_type=context.get('match_type', 'unknown'),
                                processing_time_ms=execution_time_ms
                            ))
                    except Exception:
                        pass
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def track_query_execution(
    data_source: str = "unknown",
    client: Optional[EventClient] = None,
    extract_query: Optional[Callable] = None,
    extract_results_count: Optional[Callable] = None
):
    """Decorator specifically for query execution tracking"""
    def extract_context(args, kwargs, result):
        return {"data_source": data_source}
    
    return track_execution(
        event_type='query_execution',
        client=client,
        extract_query=extract_query,
        extract_results_count=extract_results_count,
        extract_context=extract_context
    )


def track_pattern_matching(
    client: Optional[EventClient] = None,
    extract_query: Optional[Callable] = None,
    extract_pattern_info: Optional[Callable] = None
):
    """Decorator specifically for pattern matching tracking"""
    def extract_context(args, kwargs, result):
        context = {}
        if extract_pattern_info and result:
            try:
                info = extract_pattern_info(result)
                context.update(info)
            except Exception:
                pass
        return context
    
    return track_execution(
        event_type='pattern_match',
        client=client,
        extract_query=extract_query,
        extract_context=extract_context
    )


# Helper functions
def get_query_from_first_arg(args, kwargs, result):
    """Extract query from first argument"""
    return args[0] if args else ""


def get_results_count_from_list(result):
    """Extract results count from list result"""
    if isinstance(result, list):
        return len(result)
    elif isinstance(result, dict) and 'results' in result:
        return len(result['results'])
    return 0


def get_pattern_info_from_result(result):
    """Extract pattern information from result"""
    if isinstance(result, dict):
        return {
            'pattern': result.get('pattern', 'unknown'),
            'confidence': result.get('confidence', 0.0),
            'match_type': result.get('match_type', 'unknown')
        }
    return {}