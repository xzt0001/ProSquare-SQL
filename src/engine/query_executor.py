from engine.storage import Storage
from engine.index import IndexManager
import logging
from functools import cmp_to_key, partial
import os
import time
import hashlib
import json
from collections import OrderedDict
import concurrent.futures
import threading
import multiprocessing
import re
from engine.transaction import TransactionManager

# Configure logging if not already configured
logger = logging.getLogger('ProSquareSQL')

try:
    import psutil  # Used for system resource monitoring
except ImportError:
    # psutil is optional but recommended for better workload detection
    logger.warning("psutil package not installed. Using fallback workload detection. Install with: pip install psutil")
    psutil = None

class QueryExecutor:
    def __init__(self):
        """Initialize the query executor with storage."""
        self.storage = Storage()
        self.index_manager = IndexManager()
        self.transaction_manager = TransactionManager()
        
        # Define SQL NULL constant (Python's None)
        self.SQL_NULL = None
        
        # Configuration for safe operations
        self.safe_mode = True  # Set to True to enable warnings for unsafe operations
        
        # Setup operator comparison functions for WHERE clause optimization
        self._operator_funcs = {
            "=": lambda a, b: a == b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            "!=": lambda a, b: a != b,
            "<>": lambda a, b: a != b,
            "IS": lambda a, b: self._is_null_comparison(a, b, True),
            "IS NOT": lambda a, b: self._is_null_comparison(a, b, False)
        }
        
        # Operators that can use indexes for optimization
        self._indexable_operators = ["="]
        
        # Setup query cache (LRU)
        self._setup_query_cache()
        
        # Setup performance monitoring with default thresholds
        self._setup_performance_monitoring()
        
        # Setup dedicated slow query logger
        self._setup_slow_query_logger()
        
        # Join strategy preferences
        self.join_strategy_preferences = {
            "auto": True,  # Auto-select join strategy based on table sizes and data characteristics
            "hash_join_threshold": 1000,  # Number of rows above which hash join is preferred
            "size_ratio_threshold": 10,  # Size ratio that makes hash join more favorable (e.g., if one table is 10x larger)
            "nested_loop_max": 10000  # Maximum size where nested loop join is still considered
        }
        
        # Concurrency settings
        self.concurrency_preferences = {
            "auto_detect_workload": True,  # Auto-detect whether workload is CPU-bound or I/O-bound
            "min_rows_for_parallel": 1000,  # Minimum rows to consider parallelization
            "process_pool_size": multiprocessing.cpu_count(),  # Default to system CPU count
            "thread_pool_size": multiprocessing.cpu_count() * 2  # For I/O bound tasks, use more threads
        }

    def _setup_query_cache(self):
        """Initialize the query cache system with expiry support."""
        # LRU cache for query results
        self.query_cache = OrderedDict()
        # Maximum number of cached queries
        self.cache_max_size = 100
        # Maximum age of cache entries in seconds (default: 60s)
        self.cache_ttl = 60
        # Track which tables are used by which queries for cache invalidation
        self.table_to_queries = {}
        # Statistics for cache usage
        self.cache_hits = 0
        self.cache_misses = 0

    def _setup_performance_monitoring(self, slow_query_threshold=500, very_slow_query_threshold=2000):
        """Initialize the performance monitoring system with configurable thresholds.
        
        Args:
            slow_query_threshold: Warning threshold in milliseconds (default: 500ms)
            very_slow_query_threshold: Critical threshold in milliseconds (default: 2000ms)
        """
        # Thresholds for performance warnings (in milliseconds)
        self.slow_query_threshold = slow_query_threshold
        self.very_slow_query_threshold = very_slow_query_threshold
        
        # Track performance metrics
        self.query_stats = {
            "total_queries": 0,
            "slow_queries": 0,
            "table_scans": 0,
            "index_scans": 0,
            "cached_results": 0
        }
        
        # Recent slow queries for reporting
        self.recent_slow_queries = []
        self.max_slow_queries_history = 20

    def _setup_slow_query_logger(self):
        """Setup a dedicated logger for slow queries."""
        # Create a specific logger for slow queries
        self.slow_query_logger = logging.getLogger('ProSquareSQL.SlowQueries')
        
        # Check if handlers are already configured to avoid duplicates
        if not self.slow_query_logger.handlers:
            # Create directory for logs if it doesn't exist
            os.makedirs("logs", exist_ok=True)
            
            # Create a file handler that logs slow queries to a dedicated file
            slow_query_handler = logging.FileHandler("logs/slow_queries.log")
            slow_query_handler.setLevel(logging.WARNING)
            
            # Create a formatter that includes timestamp and query details
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            slow_query_handler.setFormatter(formatter)
            
            # Add the handler to the logger
            self.slow_query_logger.setLevel(logging.WARNING)
            self.slow_query_logger.addHandler(slow_query_handler)
            
            # Create transaction debug logger - separate from slow query logger
            self.tx_debug_logger = logging.getLogger('ProSquareSQL.TransactionDebug')
            if not self.tx_debug_logger.handlers:
                os.makedirs("logs", exist_ok=True)
                tx_debug_handler = logging.FileHandler("logs/transaction_debug.log")
                tx_debug_handler.setLevel(logging.DEBUG)
                tx_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                tx_debug_handler.setFormatter(tx_formatter)
                self.tx_debug_logger.setLevel(logging.DEBUG)
                self.tx_debug_logger.addHandler(tx_debug_handler)
            
            logger.info("Slow query logger configured to write to logs/slow_queries.log")
            logger.info("Transaction debug logger configured to write to logs/transaction_debug.log")

    def execute(self, query, debug=False):
        """Execute an SQL query with transaction support and optional debug mode.
        
        Args:
            query: The query to execute
            debug: Whether to enable verbose debug logging for transactions
        """
        # Start timing the query execution
        start_time = time.time()
        
        # Support both dictionary and string format for queries
        if isinstance(query, str):
            # Parse the string query
            from engine.sql_parser import SQLParser
            parser = SQLParser()
            parsed_query = parser.parse(query)
            if "error" in parsed_query:
                return {"error": parsed_query["error"]}
            query = parsed_query
        
        command = query["command"].upper()
        tokens = query["tokens"]
        
        # Log debug information if debug mode is enabled
        if debug:
            self.tx_debug_logger.debug(f"Executing query: {command} {' '.join(tokens if isinstance(tokens, list) else [])}")
            for k, v in query.items():
                if k != "tokens" and k != "command":
                    self.tx_debug_logger.debug(f"  {k}: {v}")

        # Handle transaction commands
        if command == "BEGIN" or (command == "BEGIN" and "TRANSACTION" in query.get("tokens", [])):
            tx_id = self.transaction_manager.begin_transaction()
            if debug:
                self.tx_debug_logger.debug(f"Started transaction: {tx_id}")
            return {"message": tx_id}
        elif command == "COMMIT":
            if self.transaction_manager.active_transaction:
                if debug:
                    self.tx_debug_logger.debug(f"Committing transaction: {self.transaction_manager.active_transaction}")
                    # Log operations to be committed
                    tx_info = self.transaction_manager.get_transaction_info()
                    if tx_info:
                        self.tx_debug_logger.debug(f"Transaction operations to commit: {tx_info.get('operations', [])}")
                
                result = self.transaction_manager.commit_transaction(debug_mode=debug)
                
                if debug:
                    self.tx_debug_logger.debug(f"Commit result: {result}")
                    
                return {"message": result}
            else:
                return {"error": "No active transaction to commit."}
        elif command == "ROLLBACK":
            if self.transaction_manager.active_transaction:
                if debug:
                    self.tx_debug_logger.debug(f"Rolling back transaction: {self.transaction_manager.active_transaction}")
                    # Log operations to be rolled back
                    tx_info = self.transaction_manager.get_transaction_info()
                    if tx_info:
                        self.tx_debug_logger.debug(f"Transaction operations to rollback: {tx_info.get('operations', [])}")
                
                result = self.transaction_manager.rollback_transaction(debug_mode=debug)
                
                if debug:
                    self.tx_debug_logger.debug(f"Rollback result: {result}")
                    
                return {"message": result}
            else:
                return {"error": "No active transaction to rollback."}

        # If a transaction is active, log the operation instead of executing it
        if self.transaction_manager.active_transaction:
            self.transaction_manager.log_operation(query)
            if debug:
                self.tx_debug_logger.debug(f"Queued operation in transaction {self.transaction_manager.active_transaction}: {command} {' '.join(tokens if isinstance(tokens, list) else [])}")
            return {"message": f"Query queued in transaction {self.transaction_manager.active_transaction}."}

        # For non-transaction queries, check cache for SELECT queries
        if command == "SELECT" and not self.transaction_manager.active_transaction:
            # Generate a cache key based on the query details
            cache_key = self._generate_cache_key(query)
            
            # Check if the result is in cache
            cached_result = self._get_from_cache(cache_key)
            if cached_result:
                if debug:
                    self.tx_debug_logger.debug(f"Cache hit for query: {command}")
                
                # Add execution info to cached result
                if isinstance(cached_result, dict) and "error" not in cached_result:
                    if "performance" not in cached_result:
                        cached_result["performance"] = {}
                    cached_result["performance"]["cache"] = "hit"
                    
                self.query_stats["cached_results"] += 1
                return cached_result
        
        # Execute the query as normal
        query_start_time = time.time()
        self.query_stats["total_queries"] += 1
        
        try:
            from engine.sql_parser import SQLParser
            parser = SQLParser()
            
            # Extract LIMIT for safety checks on potentially large result sets
            limit_offset = None
            if command == "SELECT":
                limit_offset = self._extract_limit_offset_clause(tokens)
                
                # Safety limit for queries without explicit LIMIT
                if not limit_offset or limit_offset.get("limit") is None:
                    # Check if this is a JOIN query to apply stricter limits
                    join_clause = self._extract_join_clause(tokens)
                    if join_clause:
                        # Apply a default safety limit for JOIN operations without explicit LIMIT
                        default_join_limit = 100000  # 100K rows limit for JOIN without explicit LIMIT
                        if not limit_offset:
                            limit_offset = {"limit": default_join_limit, "offset": 0}
                        else:
                            limit_offset["limit"] = default_join_limit
                            
                        logger.warning(f"Large JOIN operation detected without LIMIT clause. "
                                     f"Applying safety limit of {default_join_limit} rows. "
                                     f"Use explicit LIMIT to override.")
            
            if command == "SELECT":
                # Check cache for SELECT queries
                result = self.execute_select_with_cache(query)
            elif command == "INSERT":
                result = self.execute_insert(query)
                # Invalidate cache for affected table
                if "error" not in result:
                    self._invalidate_cache_for_table(parser.extract_table_name(command, tokens))
            elif command == "UPDATE":
                result = self.execute_update(query)
                # Invalidate cache for affected table
                if "error" not in result:
                    self._invalidate_cache_for_table(parser.extract_table_name(command, tokens))
            elif command == "DELETE":
                result = self.execute_delete(query)
                # Invalidate cache for affected table
                if "error" not in result:
                    self._invalidate_cache_for_table(parser.extract_table_name(command, tokens))
            elif command == "CREATE":
                result = self.execute_create(query)
            elif command == "DROP":
                result = self.execute_drop(query)
                # Invalidate cache for dropped table
                if "error" not in result:
                    self._invalidate_cache_for_table(tokens[2])
            else:
                raise ValueError(f"Unsupported command: {command}")
                
            # Calculate query execution time
            execution_time = (time.time() - query_start_time) * 1000  # Convert to ms
            
            # Track slow queries
            if execution_time > self.slow_query_threshold:
                self.query_stats["slow_queries"] += 1
                self._track_slow_query(query, execution_time)
            
            # Add performance metrics to result
            if isinstance(result, dict) and "error" not in result:
                result["performance"] = {
                    "execution_time_ms": round(execution_time, 2)
                }
                
                # Add performance warning for slow queries
                if execution_time > self.very_slow_query_threshold:
                    optimization_tips = self._generate_optimization_tips(query, execution_time)
                    result["performance"]["warning"] = f"This query took {execution_time:.2f}ms to execute, which is slow."
                    result["performance"]["optimization_tips"] = optimization_tips
                    
                    # Log the warning
                    logger.warning(f"PERFORMANCE ALERT: Slow query detected ({execution_time:.2f}ms): "
                                  f"{query.get('original_query', '')[:100]}... - {optimization_tips[0]}")
                
            # For debug mode, add detailed timing logs
            if debug:
                end_time = time.time()
                execution_time_ms = round((end_time - start_time) * 1000, 2)
                self.tx_debug_logger.debug(f"Query executed in {execution_time_ms}ms")
                self.tx_debug_logger.debug(f"Query details: {query}")
                
                # For transaction operations, log more details
                if command in ["BEGIN", "COMMIT", "ROLLBACK"]:
                    self.tx_debug_logger.debug(f"Transaction operation completed: {command}")
                
                # Add timing info to the result if it's a dict
                if isinstance(result, dict) and "error" not in result:
                    if "performance" not in result:
                        result["performance"] = {}
                    result["performance"]["debug_execution_time_ms"] = execution_time_ms
            
            # Cache the result for SELECT queries
            if command == "SELECT" and not self.transaction_manager.active_transaction:
                if "error" not in result:
                    cache_key = self._generate_cache_key(query)
                    # Extract the table name(s) for cache invalidation
                    table_name = self._extract_table_name_from_query(query)
                    self._cache_query_result(cache_key, result, table_name)
                    
                    # Add cache info to result
                    if isinstance(result, dict) and "performance" not in result:
                        result["performance"] = {}
                    result["performance"]["cache"] = "miss"
            
            return result
        except Exception as e:
            # Add context to the error message
            return {"error": f"Error executing {command} command: {str(e)}"}

    def execute_select_with_cache(self, query):
        """Execute a SELECT query with caching support."""
        # Check if parser can be imported here
        from engine.sql_parser import SQLParser
        parser = SQLParser()
        
        # Generate a cache key from the query
        cache_key = self._generate_cache_key(query)
        
        # Try to get result from cache
        op_start = time.time()
        cached_result = self._get_from_cache(cache_key)
        cache_lookup_time = (time.time() - op_start) * 1000
        
        if cached_result:
            logger.debug(f"Cache hit for query: {query.get('original_query', '')[:50]}...")
            self.cache_hits += 1
            self.query_stats["cached_results"] += 1
            
            # Add cache info to result
            if isinstance(cached_result, dict):
                cached_result["cache_info"] = {
                    "cache_hit": True,
                    "lookup_time_ms": round(cache_lookup_time, 2)
                }
            
            return cached_result
            
        # Cache miss, execute the query
        self.cache_misses += 1
        logger.debug(f"Cache miss for query: {query.get('original_query', '')[:50]}...")
        
        # Execute the query with detailed timing
        exec_start = time.time()
        
        # Extract the table name for cache invalidation tracking
        table_name = parser.extract_table_name(query["command"], query["tokens"])
        
        # Execute the query
        result = self.execute_select(query)
        exec_time = (time.time() - exec_start) * 1000
        
        # If successful, cache the result
        if "error" not in result:
            self._cache_query_result(cache_key, result, table_name)
            
            # Add cache info to result
            if isinstance(result, dict):
                result["cache_info"] = {
                    "cache_hit": False,
                    "execution_time_ms": round(exec_time, 2),
                    "cache_lookup_time_ms": round(cache_lookup_time, 2)
                }
            
        return result

    def _generate_cache_key(self, query):
        """Generate a unique key for the query to use in the cache."""
        # Use only elements that affect the result
        key_parts = {
            "command": query.get("command"),
            "tokens": query.get("tokens"),
            "components": query.get("components")
        }
        
        # Convert to a stable string representation and hash it
        query_str = json.dumps(key_parts, sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()
        
    def _get_from_cache(self, cache_key):
        """Get a query result from cache, checking expiry."""
        if cache_key in self.query_cache:
            # Get the cached entry with timestamp
            entry = self.query_cache[cache_key]
            current_time = time.time()
            
            # Check if the entry has expired
            if current_time - entry["timestamp"] > self.cache_ttl:
                # Entry expired, remove it
                del self.query_cache[cache_key]
                self.cache_misses += 1
                logger.debug(f"Cache entry expired: {cache_key}")
                return None
                
            # Move the entry to the end (most recently used)
            self.query_cache.move_to_end(cache_key)
            self.cache_hits += 1
            
            # Return the actual result
            return entry["result"]
        
        self.cache_misses += 1
        return None
        
    def _cache_query_result(self, cache_key, result, table_name):
        """Cache a query result with timestamp."""
        # Don't cache error results
        if isinstance(result, dict) and "error" in result:
            return
            
        # Create an entry with timestamp
        entry = {
            "result": result,
            "timestamp": time.time(),
            "table_name": table_name
        }
        
        # Add to cache
        self.query_cache[cache_key] = entry
        
        # Track which tables this query depends on for invalidation
        if table_name:
            if table_name not in self.table_to_queries:
                self.table_to_queries[table_name] = set()
            self.table_to_queries[table_name].add(cache_key)
            
        # Enforce cache size limit (LRU eviction)
        if len(self.query_cache) > self.cache_max_size:
            # Remove oldest entry (first item in OrderedDict)
            self.query_cache.popitem(last=False)
            
    def configure_cache(self, max_size=None, ttl=None):
        """Configure the query cache parameters."""
        if max_size is not None:
            self.cache_max_size = max_size
        if ttl is not None:
            self.cache_ttl = ttl
        logger.info(f"Query cache configured: max_size={self.cache_max_size}, ttl={self.cache_ttl}s")
        return {"max_size": self.cache_max_size, "ttl": self.cache_ttl}

    def _invalidate_cache_for_table(self, table_name):
        """Invalidate all cached queries that involve a specific table."""
        if table_name in self.table_to_queries:
            logger.info(f"Invalidating cache entries for table: {table_name}")
            
            # Get all cache keys for this table
            cache_keys = self.table_to_queries[table_name]
            
            # Remove each entry from the cache
            for key in cache_keys:
                if key in self.query_cache:
                    self.query_cache.pop(key)
                    
            # Clear the tracking set
            self.table_to_queries[table_name] = set()
    
    def get_cache_stats(self):
        """Return statistics about the query cache."""
        total_queries = self.cache_hits + self.cache_misses
        hit_ratio = (self.cache_hits / total_queries * 100) if total_queries > 0 else 0
        
        return {
            "cache_size": len(self.query_cache),
            "max_size": self.cache_max_size,
            "ttl_seconds": self.cache_ttl,
            "hits": self.cache_hits,
            "misses": self.cache_misses,
            "hit_ratio": f"{hit_ratio:.2f}%",
            "tables_tracked": len(self.table_to_queries)
        }
        
    def clear_cache(self):
        """Clear the entire query cache."""
        self.query_cache.clear()
        self.table_to_queries.clear()
        return {"message": "Query cache cleared", "entries_removed": len(self.query_cache)}

    def execute_select(self, query):
        """Execute a SELECT query with support for aggregation functions."""
        start_time = time.time()
        
        # Parse the components
        table_name = query["components"]["table"]
        join_clause = query["components"].get("join")
        columns = query["components"]["columns"]
        where = query["components"].get("where")
        group_by = query["components"].get("group_by")
        having = query["components"].get("having")
        order_by = query["components"].get("order_by")
        limit = query["components"].get("limit")
        aggregations = query["components"].get("aggregations", [])
        
        # If table doesn't exist
        if not self.storage.table_exists(table_name):
            return {"error": f"Table '{table_name}' does not exist"}
            
        # If JOIN is involved, use the _execute_join_query method
        if join_clause:
            tokens = query["tokens"]
            try:
                join_result = self._execute_join_query(table_name, join_clause, tokens, columns.split(",") if columns != "*" else ["*"])
                # Add execution time after the join
                join_result["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
                return join_result
            except Exception as e:
                return {"error": f"Error executing JOIN query: {str(e)}"}
        
        # Regular SELECT query processing
        # Track operations for performance analysis
        performance_data = {
            "operation_timings": {},
            "table_scan_count": 0,
            "index_scan_count": 0,
            "rows_processed": 0
        }
        
        # Get all data from the table
        op_start = time.time()
        data = self.storage.get_table(table_name)
        performance_data["operation_timings"]["load_table"] = (time.time() - op_start) * 1000
        
        # If table is empty, we can't validate columns
        if not data:
            return {"result": [], "message": "0 records retrieved (table is empty)"}
            
        performance_data["rows_processed"] = len(data)
        
        # Get table schema from first record
        table_schema = list(data[0].keys()) if data else []
        
        # Process projections (columns)
        column_list = []
        if columns != "*":
            column_list = [col.strip() for col in columns.split(",")]
            
            # Validate column names before processing
            invalid_columns = [col for col in column_list if col not in table_schema]
            if invalid_columns:
                return {"error": f"Column(s) not found in table '{table_name}': {', '.join(invalid_columns)}"}
        
        # Process WHERE clause and validate any column references
        if where:
            # Parse where clause into a structured format for filtering
            where_parts = where.split()
            if len(where_parts) < 3:
                return {"error": f"Invalid WHERE clause: {where}"}
                
            field = where_parts[0]
            operator = where_parts[1]
            value = " ".join(where_parts[2:])
            
            # Try to convert value to appropriate type
            try:
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]  # Remove quotes
                elif value.lower() == "null":
                    value = None
                elif value.isdigit():
                    value = int(value)
                elif value.replace(".", "", 1).isdigit():
                    value = float(value)
            except Exception:
                pass
                
            where_clause = {
                "field": field,
                "operator": operator,
                "value": value
            }
            
            if field not in table_schema:
                return {"error": f"WHERE clause references column '{field}' which does not exist in table '{table_name}'"}
                
            # Check if we could use an index but none exists
            indexable_operators = ["=", "==", "<>", "!=", ">", "<", ">=", "<="]
            should_use_index = operator in indexable_operators
            has_index = self._can_use_index(table_name, where_clause)
            
            if should_use_index:
                if not has_index:
                    logger.warning(f"PERFORMANCE WARNING: Full table scan performed on '{table_name}'. "
                                  f"Query on column '{field}' would be faster with an index.")
                    performance_data["table_scan_count"] += 1
                    self.query_stats["table_scans"] += 1
                else:
                    performance_data["index_scan_count"] += 1
                    self.query_stats["index_scans"] += 1
            
            # Try to use index for filtering if possible
            op_start = time.time()
            if should_use_index and has_index:
                logger.info(f"Using index on {table_name}.{field} for query")
                data = self._filter_with_index(table_name, data, where_clause)
            else:
                data = self._filter_data(data, where_clause)
            performance_data["operation_timings"]["filter_data"] = (time.time() - op_start) * 1000
        
        # Group data if GROUP BY is specified
        if group_by:
            # Validate group by columns exist
            group_by_columns = group_by.split(",")
            for col in group_by_columns:
                if col not in table_schema:
                    return {"error": f"GROUP BY references column '{col}' which does not exist in table '{table_name}'"}
            data = self._group_data(data, group_by)
        
        # Apply aggregation functions
        if aggregations:
            data = self._apply_aggregations(data, aggregations)
        
        # Process HAVING clause
        if having:
            data = self._filter_having(data, having)
        
        # Apply ORDER BY if present
        if order_by:
            op_start = time.time()
            order_parts = order_by.split()
            order_column = order_parts[0]
            order_direction = "ASC"
            if len(order_parts) > 1:
                if order_parts[1].upper() in ["ASC", "DESC"]:
                    order_direction = order_parts[1].upper()
            
            order_clause = {
                "column": order_column,
                "direction": order_direction
            }
            
            # Validate order by column exists
            if order_column not in table_schema:
                return {"error": f"ORDER BY references column '{order_column}' which does not exist in table '{table_name}'"}
                
            data = self._sort_data(data, order_clause)
            performance_data["operation_timings"]["sort_data"] = (time.time() - op_start) * 1000
            
        # Apply LIMIT if present
        if limit:
            op_start = time.time()
            limit_clause = {
                "limit": limit,
                "offset": 0
            }
            data = self._apply_limit_offset(data, limit_clause)
            performance_data["operation_timings"]["apply_limit"] = (time.time() - op_start) * 1000
            
        # Apply column projection if needed
        if column_list:
            op_start = time.time()
            data = self._project_columns(data, column_list)
            performance_data["operation_timings"]["project_columns"] = (time.time() - op_start) * 1000
            
        # Replace None values with SQL_NULL indicator in result
        self._normalize_null_values(data)
        
        result = {"result": data, "message": f"{len(data)} records retrieved"}
        
        # Add performance data to the result
        result["_performance_data"] = performance_data
        
        # Calculate and add execution time
        execution_time = (time.time() - start_time) * 1000
        result["execution_time_ms"] = round(execution_time, 2)
        
        # Log warnings for slow SELECT queries
        slow_query_threshold = 100  # Set threshold for slow query logging
        if execution_time > self.very_slow_query_threshold:
            # Critical slow query
            warning_msg = (
                f"⚠️ CRITICAL: Slow SELECT query ({execution_time:.2f}ms) on table '{table_name}'. " + 
                f"Rows processed: {len(data)}."
            )
            logger.warning(warning_msg)
            self.slow_query_logger.warning(warning_msg)
        
        elif execution_time > self.slow_query_threshold and execution_time > slow_query_threshold:
            # Moderate slow query
            warning_msg = f"⚠️ WARNING: Slow SELECT query ({execution_time:.2f}ms) on table '{table_name}'." 
            logger.warning(warning_msg)
            self.slow_query_logger.warning(warning_msg)
        
        return result

    def _apply_aggregations(self, data, aggregations):
        """Apply aggregation functions to the data."""
        # Example implementation of aggregation logic
        aggregated_data = []
        for group in data:
            aggregated_row = {}
            for agg in aggregations:
                func, col = re.match(r"(SUM|AVG|COUNT|MIN|MAX)\s*\((.+)\)", agg, re.IGNORECASE).groups()
                if func.upper() == "SUM":
                    aggregated_row[agg] = sum(row[col] for row in group)
                elif func.upper() == "AVG":
                    aggregated_row[agg] = sum(row[col] for row in group) / len(group)
                elif func.upper() == "COUNT":
                    aggregated_row[agg] = len(group)
                elif func.upper() == "MIN":
                    aggregated_row[agg] = min(row[col] for row in group)
                elif func.upper() == "MAX":
                    aggregated_row[agg] = max(row[col] for row in group)
            aggregated_data.append(aggregated_row)
        return aggregated_data

    def _group_data(self, data, group_by):
        """Group data by specified columns."""
        grouped_data = {}
        for row in data:
            key = tuple(row[col] for col in group_by.split(","))
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(row)
        return list(grouped_data.values())

    def _filter_having(self, data, having):
        """Filter data based on HAVING clause with proper type conversion for comparison."""
        # Example implementation of HAVING logic
        filtered_data = []
        for group in data:
            # Assuming HAVING clause is a simple condition like "SUM(col) > 10"
            func, col, operator, value = re.match(r"(SUM|AVG|COUNT|MIN|MAX)\s*\((.+)\)\s*(>|<|=|>=|<=)\s*(.+)", having, re.IGNORECASE).groups()
            if func.upper() == "SUM":
                agg_value = sum(row[col] for row in group)
            elif func.upper() == "AVG":
                agg_value = sum(row[col] for row in group) / len(group)
            elif func.upper() == "COUNT":
                agg_value = len(group)
            elif func.upper() == "MIN":
                agg_value = min(row[col] for row in group)
            elif func.upper() == "MAX":
                agg_value = max(row[col] for row in group)
            
            # Convert value to appropriate type for comparison
            try:
                if value.isdigit():
                    value = int(value)
                else:
                    value = float(value)
            except ValueError:
                logger.error(f"Invalid HAVING value: {value}")
                continue
            
            # Evaluate the condition
            if eval(f"{agg_value} {operator} {value}"):
                filtered_data.append(group)
        return filtered_data

    def execute_insert(self, query):
        """Executes an INSERT query."""
        tokens = query["tokens"]
        from engine.sql_parser import SQLParser
        parser = SQLParser()
        
        try:
            table_name = parser.extract_table_name(query["command"], tokens)
            
            # Check if table exists
            if not self.storage.table_exists(table_name):
                return {"error": f"Table '{table_name}' does not exist"}
                
            # Parse the VALUES clause
            values = self._extract_values_clause(tokens)
            if not values:
                return {"error": "No values specified for INSERT"}
                
            # Get columns if specified
            columns = self._extract_insert_columns(tokens)
            
            # Get existing data to check schema
            data = self.storage.get_table(table_name)
            
            # Validate columns against existing schema
            if data and columns:
                table_schema = list(data[0].keys())
                invalid_columns = [col for col in columns if col not in table_schema and col != "id"]
                if invalid_columns:
                    return {"error": f"Column(s) not found in table '{table_name}': {', '.join(invalid_columns)}"}
                
            # Create a new record
            new_record = {}
            
            # Handle explicit column names
            if columns:
                if len(columns) != len(values):
                    return {"error": f"Column count ({len(columns)}) does not match value count ({len(values)})"}
                    
                for i, col in enumerate(columns):
                    new_record[col] = values[i]
            else:
                # For schema-less insertion, check if existing records have a schema to follow
                if data:
                    table_schema = list(data[0].keys())
                    # Remove 'id' from schema if present since we'll handle it separately
                    if "id" in table_schema:
                        table_schema.remove("id")
                        
                    # Check if value count matches schema
                    if len(values) != len(table_schema):
                        return {"error": f"Table expects {len(table_schema)} values, but {len(values)} were provided"}
                        
                    # Map values to schema columns
                    for i, col in enumerate(table_schema):
                        if i < len(values):
                            new_record[col] = values[i]
                else:
                    # No schema exists, create generic column names
                    for i, val in enumerate(values):
                        new_record[f"col{i+1}"] = val
            
            # Add an ID if not present
            if "id" not in new_record and data:
                # Find max ID and increment
                max_id = max((record.get("id", 0) for record in data), default=0)
                new_record["id"] = max_id + 1
            elif "id" not in new_record:
                new_record["id"] = 1
                
            # Add the record
            data.append(new_record)
            
            # Automatically create a backup before saving
            if data:
                self.storage.backup_table(table_name)
                
            # Save the updated table
            self.storage.save_table(table_name, data)
            
            # Update indexes for the new record
            self._update_indexes_for_insert(table_name, new_record)
            
            # Handle NULL values in the response
            self._normalize_null_values([new_record])
            
            # At the very end, right before returning the result:
            self._invalidate_cache_for_table(table_name)
            return {"message": "1 record inserted", "record": new_record}
        except Exception as e:
            return {"error": str(e)}

    def execute_update(self, query):
        """Executes an UPDATE query."""
        tokens = query["tokens"]
        from engine.sql_parser import SQLParser
        parser = SQLParser()
        
        try:
            table_name = parser.extract_table_name(query["command"], tokens)
            
            # Check if table exists
            if not self.storage.table_exists(table_name):
                return {"error": f"Table '{table_name}' does not exist"}
                
            # Get SET clause
            set_clause = self._extract_set_clause(tokens)
            if not set_clause:
                return {"error": "No SET clause found in UPDATE query"}
                
            # Get table data
            data = self.storage.get_table(table_name)
            
            # Validate columns in SET clause
            if data:
                table_schema = list(data[0].keys())
                invalid_columns = [col for col in set_clause.keys() if col not in table_schema]
                if invalid_columns:
                    return {"error": f"Column(s) not found in table '{table_name}': {', '.join(invalid_columns)}"}
            else:
                return {"error": f"Table '{table_name}' is empty, cannot update records"}
                
            # Get WHERE clause
            where_clause = self._extract_where_clause(tokens)
            
            # Safety check for missing WHERE clause
            if not where_clause and self.safe_mode:
                warning_msg = f"WARNING: You are about to update ALL records in table '{table_name}'. This can't be undone."
                # Log with appropriate level
                logger.warning(warning_msg)
                # Add operation details for traceability
                logger.info(f"Unsafe UPDATE operation detected on table '{table_name}' - waiting for confirmation")
                
                return {
                    "error": warning_msg, 
                    "require_confirmation": True, 
                    "operation": "UPDATE", 
                    "table": table_name,
                    "timestamp": self._get_timestamp()
                }
                
            # Validate column in WHERE clause
            if where_clause:
                field = where_clause["field"]
                if field not in table_schema:
                    return {"error": f"WHERE clause references column '{field}' which does not exist in table '{table_name}'"}
            
            # Automatically create a backup before updating
            self.storage.backup_table(table_name)
            
            # Try to use index for filtering if possible
            records_to_update = []
            if where_clause and self._can_use_index(table_name, where_clause):
                logger.info(f"Using index on {table_name}.{where_clause['field']} for UPDATE")
                records_to_update = self._filter_with_index(table_name, data, where_clause)
            else:
                # If no index or can't use index, check each record
                records_to_update = [record for record in data if not where_clause or self._record_matches_where(record, where_clause)]
            
            # Apply updates and track index changes
            updated_count = 0
            for record in records_to_update:
                # Keep track of old values for index updates
                old_values = {field: record.get(field) for field in set_clause.keys()}
                
                # Update record fields
                for field, value in set_clause.items():
                    # Type validation - ensure value is compatible with existing data
                    if field in record and record[field] is not None and value is not None:
                        if not self._is_compatible_type(record[field], value):
                            return {"error": f"Type mismatch: Cannot update field '{field}' with incompatible value type"}
                    record[field] = value
                
                # Update indexes for changed fields
                self._update_indexes_for_update(table_name, record, old_values)
                    
                updated_count += 1
            
            # Save updated data
            self.storage.save_table(table_name, data)
            
            # Log the successful update
            logger.info(f"Updated {updated_count} records in table '{table_name}'")
            
            return {"message": f"{updated_count} records updated"}
        except Exception as e:
            logger.error(f"Error executing UPDATE on table '{table_name}': {str(e)}")
            return {"error": str(e)}

    def execute_delete(self, query):
        """Executes a DELETE query."""
        tokens = query["tokens"]
        from engine.sql_parser import SQLParser
        parser = SQLParser()
        
        try:
            table_name = parser.extract_table_name(query["command"], tokens)
            
            # Check if table exists
            if not self.storage.table_exists(table_name):
                return {"error": f"Table '{table_name}' does not exist"}
                
            # Get WHERE clause
            where_clause = self._extract_where_clause(tokens)
            
            # Safety check for missing WHERE clause
            if not where_clause and self.safe_mode:
                warning_msg = f"WARNING: You are about to delete ALL records in table '{table_name}'. This can't be undone."
                # Log with appropriate level
                logger.warning(warning_msg)
                # Add operation details for traceability
                logger.info(f"Unsafe DELETE operation detected on table '{table_name}' - waiting for confirmation")
                
                return {
                    "error": warning_msg, 
                    "require_confirmation": True, 
                    "operation": "DELETE", 
                    "table": table_name,
                    "timestamp": self._get_timestamp()
                }
            
            # Get table data
            data = self.storage.get_table(table_name)
            
            # Validate column in WHERE clause
            if where_clause and data:
                table_schema = list(data[0].keys())
                field = where_clause["field"]
                if field not in table_schema:
                    return {"error": f"WHERE clause references column '{field}' which does not exist in table '{table_name}'"}
            
            original_count = len(data)
            
            # Automatically create a backup before deleting
            self.storage.backup_table(table_name)
            
            records_to_delete = []
            remaining_records = []
            
            if where_clause:
                # Try to use index if possible
                if self._can_use_index(table_name, where_clause):
                    logger.info(f"Using index on {table_name}.{where_clause['field']} for DELETE")
                    records_to_delete = self._filter_with_index(table_name, data, where_clause)
                    # Create new list without deleted records
                    remaining_records = [record for record in data if record not in records_to_delete]
                else:
                    # Traditional approach - find records to keep
                    for record in data:
                        if self._record_matches_where(record, where_clause):
                            records_to_delete.append(record)
                        else:
                            remaining_records.append(record)
                
                deleted_count = len(records_to_delete)
                data = remaining_records
            else:
                # Delete all records
                records_to_delete = data.copy()
                deleted_count = len(data)
                data = []
            
            # Update indexes for deleted records
            self._update_indexes_for_delete(table_name, records_to_delete)
            
            # Save updated data
            self.storage.save_table(table_name, data)
            
            # Log the successful deletion
            logger.info(f"Deleted {deleted_count} records from table '{table_name}'")
            
            return {"message": f"{deleted_count} records deleted"}
        except Exception as e:
            logger.error(f"Error executing DELETE on table '{table_name}': {str(e)}")
            return {"error": str(e)}
            
    def execute_create(self, query):
        """Executes a CREATE TABLE query."""
        tokens = query["tokens"]
        
        try:
            # Verify syntax
            if len(tokens) < 3 or tokens[1].upper() != "TABLE":
                return {"error": "Invalid CREATE statement syntax. Expected: CREATE TABLE table_name"}
                
            table_name = tokens[2]
            
            # Check if table already exists
            if self.storage.table_exists(table_name):
                return {"error": f"Table '{table_name}' already exists"}
                
            # Create empty table
            self.storage.save_table(table_name, [])
            
            # FIX 3: Clearly indicate that primary key indexing will happen automatically
            logger.info(f"Table '{table_name}' created. Primary key 'id' will be automatically indexed when data is inserted.")
            
            return {"message": f"Table '{table_name}' created successfully"}
        except Exception as e:
            return {"error": str(e)}
            
    def execute_drop(self, query):
        """Executes a DROP TABLE query."""
        tokens = query["tokens"]
        
        try:
            # Verify syntax
            if len(tokens) < 3 or tokens[1].upper() != "TABLE":
                return {"error": "Invalid DROP statement syntax. Expected: DROP TABLE table_name"}
                
            table_name = tokens[2]
            
            # Check if table exists
            if not self.storage.table_exists(table_name):
                return {"error": f"Table '{table_name}' does not exist"}
                
            # Automatically create a backup before dropping
            self.storage.backup_table(table_name)
            
            # Drop the table
            self.storage.delete_table(table_name)
            
            return {"message": f"Table '{table_name}' dropped successfully"}
        except Exception as e:
            return {"error": str(e)}
    
    # Helper methods for query processing
    def _normalize_null_values(self, data):
        """Replaces None values with SQL NULL indicator in records."""
        for record in data:
            for key, value in record.items():
                if value is None:
                    record[key] = "NULL"  # Use string "NULL" for display
    
    def _extract_columns(self, tokens):
        """Extract column list from SELECT query."""
        # Start from beginning until FROM
        columns = []
        i = 1  # Skip SELECT
        while i < len(tokens) and tokens[i].upper() != "FROM":
            col = tokens[i]
            if col == ",":
                pass  # Skip commas
            else:
                columns.append(col)
            i += 1
            
        return columns
        
    def _extract_where_clause(self, tokens):
        """Extract WHERE clause from query tokens."""
        # Find WHERE keyword
        try:
            where_index = [i for i, token in enumerate(tokens) if token.upper() == "WHERE"]
            if not where_index:
                return None
                
            where_index = where_index[0]
            # Simple implementation assuming format: field operator value
            if where_index + 3 < len(tokens):
                return {
                    "field": tokens[where_index + 1],
                    "operator": tokens[where_index + 2],
                    "value": tokens[where_index + 3]
                }
            return None
        except Exception:
            return None
            
    def _extract_order_by_clause(self, tokens):
        """Extract ORDER BY clause from query tokens."""
        # Find ORDER BY keywords
        try:
            order_index = -1
            for i, token in enumerate(tokens):
                if token.upper() == "ORDER" and i + 1 < len(tokens) and tokens[i + 1].upper() == "BY":
                    order_index = i
                    break
                    
            if order_index == -1:
                return None
                
            # Get column name and direction
            if order_index + 2 < len(tokens):
                column = tokens[order_index + 2]
                
                # Check for optional ASC/DESC
                direction = "ASC"  # Default is ascending
                if order_index + 3 < len(tokens):
                    if tokens[order_index + 3].upper() in ["ASC", "DESC"]:
                        direction = tokens[order_index + 3].upper()
                
                return {
                    "column": column,
                    "direction": direction
                }
            return None
        except Exception as e:
            logger.error(f"Error extracting ORDER BY clause: {str(e)}")
            return None
            
    def _extract_limit_offset_clause(self, tokens):
        """Extract LIMIT and optional OFFSET from query tokens."""
        try:
            # Find LIMIT keyword
            limit_index = -1
            for i, token in enumerate(tokens):
                if token.upper() == "LIMIT":
                    limit_index = i
                    break
                    
            if limit_index == -1:
                return None
                
            # Process LIMIT
            limit = None
            if limit_index + 1 < len(tokens):
                try:
                    limit = int(tokens[limit_index + 1])
                except ValueError:
                    raise ValueError(f"Invalid LIMIT value: {tokens[limit_index + 1]}")
            
            # Check for OFFSET
            offset = 0  # Default is no offset
            for i, token in enumerate(tokens):
                if token.upper() == "OFFSET":
                    if i + 1 < len(tokens):
                        try:
                            offset = int(tokens[i + 1])
                        except ValueError:
                            raise ValueError(f"Invalid OFFSET value: {tokens[i + 1]}")
                    break
                    
            if limit is not None:
                return {
                    "limit": limit,
                    "offset": offset
                }
            return None
        except Exception as e:
            logger.error(f"Error extracting LIMIT/OFFSET clause: {str(e)}")
            return None
            
    def _extract_set_clause(self, tokens):
        """Extract SET clause from UPDATE query."""
        set_clause = {}
        try:
            set_index = [i for i, token in enumerate(tokens) if token.upper() == "SET"]
            if not set_index:
                return None
                
            set_index = set_index[0]
            i = set_index + 1
            
            # Extract all field=value pairs
            current_field = None
            expecting_equals = False
            
            while i < len(tokens) and tokens[i].upper() not in ["WHERE", "ORDER", "LIMIT"]:
                token = tokens[i]
                if token == ",":
                    pass  # Skip commas
                elif expecting_equals and token == "=":
                    expecting_equals = False
                elif not expecting_equals and current_field:
                    # This is a value
                    set_clause[current_field] = self._process_value(token)
                    current_field = None
                else:
                    # This is a field name
                    current_field = token
                    expecting_equals = True
                i += 1
                
            return set_clause
        except Exception:
            return {}
            
    def _extract_values_clause(self, tokens):
        """Extract VALUES clause from INSERT query."""
        try:
            values_index = [i for i, token in enumerate(tokens) if token.upper() == "VALUES"]
            if not values_index:
                return None
                
            values_index = values_index[0]
            values = []
            
            # Handle parentheses
            if values_index + 1 < len(tokens) and tokens[values_index + 1] == "(":
                i = values_index + 2  # Skip VALUES and first (
                while i < len(tokens) and tokens[i] != ")":
                    if tokens[i] != ",":
                        values.append(self._process_value(tokens[i]))
                    i += 1
                    
            return values
        except Exception:
            return None
            
    def _extract_insert_columns(self, tokens):
        """Extract column names from INSERT query."""
        try:
            into_index = [i for i, token in enumerate(tokens) if token.upper() == "INTO"]
            if not into_index:
                return None
                
            # Look for opening parenthesis after table name
            table_index = into_index[0] + 1
            if table_index + 1 < len(tokens) and tokens[table_index + 1] == "(":
                columns = []
                i = table_index + 2  # Skip table name and first (
                while i < len(tokens) and tokens[i] != ")":
                    if tokens[i] != ",":
                        columns.append(tokens[i])
                    i += 1
                return columns
            return None
        except Exception:
            return None
            
    def _process_value(self, token):
        """Process a value token to convert it to the appropriate type."""
        # Handle NULL values (case-insensitive)
        if isinstance(token, str) and token.upper() == "NULL":
            return self.SQL_NULL
        
        # Remove quotes from string values
        if isinstance(token, str) and ((token.startswith("'") and token.endswith("'")) or 
                                       (token.startswith('"') and token.endswith('"'))):
            return token[1:-1]
            
        # Try to convert to number
        if isinstance(token, str):
            try:
                if "." in token:
                    return float(token)
                return int(token)
            except ValueError:
                # If not a number, return as is
                return token
        
        # Return non-string tokens as is
        return token
            
    def _filter_data(self, data, where_clause):
        """Filter data based on where clause - optimized version."""
        field = where_clause["field"]
        operator = where_clause["operator"].upper()  # Normalize operator case
        value = self._process_value(where_clause["value"])
        
        # Get the appropriate comparison function
        compare_func = self._operator_funcs.get(operator)
        if not compare_func:
            logger.warning(f"Unknown operator '{operator}' in WHERE clause")
            return []
            
        # Use a list comprehension with single filtering function for better performance
        return [
            record for record in data
            if self._evaluate_where_condition(record, field, operator, value, compare_func)
        ]
    
    def _evaluate_where_condition(self, record, field, operator, value, compare_func):
        """Evaluate a WHERE condition for a record using the appropriate comparison function."""
        # If field doesn't exist, handle specially for IS NULL
        if field not in record:
            return operator == "IS" and (value is None or value == "NULL")
                
        record_value = record[field]
        record_is_null = (record_value is None or record_value == "NULL")
        value_is_null = (value is None or value == "NULL")
        
        # Special handling for NULL values in comparisons
        if record_is_null or value_is_null:
            if operator in ["IS", "IS NOT"]:
                return compare_func(record_value, value)
            else:
                return False  # All other comparisons with NULL return false
                
        # For non-NULL values, use the comparison function
        try:
            return compare_func(record_value, value)
        except (TypeError, ValueError) as e:
            # Handle comparison errors gracefully
            logger.debug(f"Comparison error: {str(e)} - treating as non-match")
            return False
    
    def _is_null_comparison(self, record_value, value, is_equality):
        """Handle IS NULL and IS NOT NULL comparisons."""
        record_is_null = (record_value is None or record_value == "NULL")
        value_is_null = (value is None or value == "NULL")
        
        if is_equality:  # IS NULL
            return value_is_null and record_is_null
        else:  # IS NOT NULL
            return value_is_null and not record_is_null
    
    def _sort_data(self, data, order_by):
        """Sort data based on ORDER BY clause using cmp_to_key for better performance with mixed types."""
        if not data:
            return data
            
        column = order_by["column"]
        direction = order_by["direction"]
        
        # Define a comparison function that handles NULL values and type mismatches
        def compare_values(a, b):
            # Extract values to compare
            a_val = a.get(column) if column in a else None
            b_val = b.get(column) if column in b else None
            
            # Handle NULL values
            a_is_null = (a_val is None or a_val == "NULL")
            b_is_null = (b_val is None or b_val == "NULL")
            
            # NULLs come first in ASC, last in DESC
            if a_is_null and b_is_null:
                return 0
            if a_is_null:
                return -1 if direction == "ASC" else 1
            if b_is_null:
                return 1 if direction == "ASC" else -1
                
            # Handle type mismatches by converting to strings
            if type(a_val) != type(b_val):
                try:
                    # Try direct comparison first (might work for compatible types)
                    result = -1 if a_val < b_val else (1 if a_val > b_val else 0)
                    return result if direction == "ASC" else -result
                except TypeError:
                    # Fall back to string comparison
                    a_str = str(a_val).lower() if isinstance(a_val, str) else str(a_val)
                    b_str = str(b_val).lower() if isinstance(b_val, str) else str(b_val)
                    result = -1 if a_str < b_str else (1 if a_str > b_str else 0)
                    return result if direction == "ASC" else -result
            
            # For same types, compare directly
            if a_val == b_val:
                return 0
                
            # String comparison should be case-insensitive
            if isinstance(a_val, str):
                a_val = a_val.lower()
                b_val = b_val.lower()
                
            # Return -1 if a < b, 1 if a > b
            result = -1 if a_val < b_val else 1
            
            # Invert for DESC order
            return result if direction == "ASC" else -result
            
        # Use cmp_to_key to convert our comparison function for use with sorted()
        try:
            return sorted(data, key=cmp_to_key(compare_values))
        except Exception as e:
            logger.warning(f"Advanced sorting failed: {str(e)}. Falling back to basic sorting.")
            
            # Fall back to basic sorting if advanced sorting fails
            def basic_sort_key(record):
                value = record.get(column) if column in record else None
                if value is None or value == "NULL":
                    return (0 if direction == "ASC" else 1, "")
                return (1 if direction == "ASC" else 0, str(value))
                
            return sorted(data, key=basic_sort_key, reverse=(direction == "DESC"))
    
    def _record_matches_where(self, record, where_clause):
        """Check if a record matches the WHERE clause conditions - optimized version."""
        field = where_clause["field"]
        operator = where_clause["operator"].upper()  # Normalize operator case
        value = self._process_value(where_clause["value"])
        
        # Get the appropriate comparison function
        compare_func = self._operator_funcs.get(operator)
        if not compare_func:
            logger.warning(f"Unknown operator '{operator}' in WHERE clause")
            return False
            
        return self._evaluate_where_condition(record, field, operator, value, compare_func)
    
    def _get_timestamp(self):
        """Get a formatted timestamp for logging purposes."""
        import datetime
        return datetime.datetime.now().isoformat()

    # Index-related methods
    def _can_use_index(self, table_name, where_clause):
        """Determines if an index can be used for the given WHERE clause."""
        # Only equality operators can use hash indexes for now
        if where_clause["operator"] not in self._indexable_operators:
            return False
            
        # Check if an index exists for this table and column
        field = where_clause["field"]
        index_file = self.index_manager.get_index_file(table_name, field)
        
        return os.path.exists(index_file)
        
    def _filter_with_index(self, table_name, data, where_clause):
        """Uses an index to filter data based on WHERE clause."""
        field = where_clause["field"]
        value = self._process_value(where_clause["value"])
        
        # Get matching row IDs from index
        matching_ids = self.index_manager.lookup(table_name, field, value)
        
        if not matching_ids:
            return []
            
        # Retrieve matching records from data
        result = []
        id_set = set(matching_ids)  # Convert to set for O(1) lookups
        
        for record in data:
            if record.get("id") in id_set:
                result.append(record)
                
        return result
        
    def _update_indexes_for_insert(self, table_name, record):
        """Updates indexes when a new record is inserted."""
        # Get table data to check which columns are indexed
        data = self.storage.get_table(table_name)
        if not data or len(data) <= 1:  # Only one record (the one we just inserted)
            return
            
        # FIX 3: Always create an index on the primary key (id) field
        if "id" in record:
            logger.info(f"Creating/updating primary key index on {table_name}.id")
            self.index_manager.create_index(table_name, "id", data)
            
        # Create indexes for other common columns that might need them
        for column in ["name", "email", "username"]:
            if column in record:
                # Check if the column exists in the table schema
                if column in data[0]:
                    # Create or update the index for this column
                    self.index_manager.create_index(table_name, column, data)
                    
    def _update_indexes_for_update(self, table_name, record, old_values):
        """Updates indexes when a record is updated."""
        # Only need to update indexes for columns that changed
        row_id = record.get("id")
        if row_id is None:
            return
            
        for column, old_value in old_values.items():
            new_value = record.get(column)
            
            # FIX 2: Explicit check to avoid unnecessary index updates
            if old_value == new_value:
                # Skip this column as the value hasn't changed
                continue
                
            # Check if an index exists for this column
            index_file = self.index_manager.get_index_file(table_name, column)
            if os.path.exists(index_file):
                # Update the index
                self.index_manager.update_index(table_name, column, old_value, new_value, row_id)
                
    def _update_indexes_for_delete(self, table_name, records):
        """Updates indexes when records are deleted."""
        # Get all indexed columns
        indexed_columns = set()
        data_dir = os.path.join("data", "indexes")
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.startswith(f"{table_name}_") and filename.endswith(".json"):
                    column = filename[len(table_name) + 1:-5]  # Extract column name from filename
                    indexed_columns.add(column)
        
        # Update each index
        for record in records:
            row_id = record.get("id")
            if row_id is None:
                continue
                
            for column in indexed_columns:
                if column in record:
                    value = record[column]
                    self.index_manager.delete_from_index(table_name, column, value, row_id)

    # Additional helper methods
    def _project_columns(self, data, columns):
        """Projects specific columns from the data."""
        return [{col: row.get(col) for col in columns} for row in data]
        
    def _apply_limit_offset(self, data, limit_offset):
        """Applies LIMIT and OFFSET to the data."""
        offset = limit_offset.get("offset", 0)
        limit = limit_offset.get("limit")
        
        if offset >= len(data):
            return []
            
        if limit is not None:
            return data[offset:offset + limit]
        return data[offset:]
        
    def _is_compatible_type(self, original, new_value):
        """Checks if a new value is compatible with the original type."""
        # None can be assigned to any type
        if new_value is None:
            return True
            
        # If original is None, any type is compatible
        if original is None:
            return True
            
        # For basic types, check if they match
        if isinstance(original, (int, float)) and isinstance(new_value, (int, float)):
            return True
        if isinstance(original, str) and isinstance(new_value, str):
            return True
        if isinstance(original, bool) and isinstance(new_value, bool):
            return True
            
        # For any other case, check if types match exactly
        return type(original) == type(new_value)

    # JOIN-related methods
    def _extract_join_clause(self, tokens):
        """Extract join details from query tokens."""
        join_keywords = ["JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"]
        join_info = None
        
        # Normalize tokens for easier matching
        tokens_upper = [t.upper() if isinstance(t, str) else t for t in tokens]
        
        for i, token in enumerate(tokens_upper):
            # Find JOIN keyword
            if token in join_keywords or (i > 0 and f"{tokens_upper[i-1]} {token}" in join_keywords):
                # Extract join details
                if i + 3 < len(tokens) and tokens_upper[i+2] == "ON":
                    join_type = token
                    join_table = tokens[i+1]
                    
                    # Parse the ON condition (assuming format: table1.column = table2.column)
                    on_condition_str = ""
                    j = i + 3
                    while j < len(tokens) and tokens_upper[j] not in ["WHERE", "GROUP", "ORDER", "LIMIT"]:
                        on_condition_str += f"{tokens[j]} "
                        j += 1
                    
                    # Parse the ON condition components
                    on_parts = on_condition_str.split("=")
                    if len(on_parts) == 2:
                        left_side = on_parts[0].strip().split(".")
                        right_side = on_parts[1].strip().split(".")
                        
                        if len(left_side) == 2 and len(right_side) == 2:
                            return {
                                "type": join_type,
                                "table": join_table,
                                "left_table": left_side[0],
                                "left_column": left_side[1],
                                "right_table": right_side[0],
                                "right_column": right_side[1]
                            }
        
        return None
        
    def _execute_join_query(self, main_table, join_clause, tokens, columns=None):
        """Execute a JOIN query with optimizations."""
        # Start timing the join query execution
        join_start_time = time.time()
        
        # Check if tables exist
        join_table = join_clause["table"]
        if not self.storage.table_exists(main_table):
            return {"error": f"Table '{main_table}' does not exist"}
        if not self.storage.table_exists(join_table):
            return {"error": f"Join table '{join_table}' does not exist"}
            
        # Load data from both tables
        main_data = self.storage.get_table(main_table)
        join_data = self.storage.get_table(join_table)
        
        # Get the JOIN type
        join_type = join_clause.get("type", "INNER JOIN")
        
        # Get table aliases if any
        main_alias = join_clause.get("main_alias")
        join_alias = join_clause.get("alias")
        
        # Use aliases in result column prefixes if provided
        main_prefix = main_alias if main_alias else main_table
        join_prefix = join_alias if join_alias else join_table
        
        if not main_data and not join_data:
            return {"result": [], "message": "0 records retrieved (both tables are empty)"}
        elif not main_data and join_type in ["RIGHT JOIN", "FULL JOIN"]:
            # For RIGHT JOIN and FULL JOIN with empty left table, we still need to process
            pass
        elif not join_data and join_type in ["LEFT JOIN", "FULL JOIN"]:
            # For LEFT JOIN and FULL JOIN with empty right table, we still need to process
            pass
        elif not main_data or not join_data:
            return {"result": [], "message": "0 records retrieved (one table is empty)"}
            
        # Handle CROSS JOIN separately as it doesn't need conditions
        if join_type == "CROSS JOIN":
            # Detect potentially inefficient CROSS JOIN that could be an INNER JOIN
            if join_clause.get("condition"):
                logger.warning(f"⚠️ OPTIMIZATION WARNING: CROSS JOIN with ON condition detected. "
                             f"Consider using INNER JOIN instead for better performance.")
                logger.info(f"Suggestion: Replace 'CROSS JOIN {join_table} ON {join_clause['condition']}' "
                          f"with 'INNER JOIN {join_table} ON {join_clause['condition']}'")
            
            return self._execute_cross_join(main_table, join_table, main_data, join_data, tokens, columns)
            
        # Parse the join condition
        condition = join_clause.get("condition")
        if not condition and join_type != "CROSS JOIN":
            return {"error": f"Missing ON condition for {join_type}"}
            
        # Extract join condition columns
        condition_parts = condition.split("=")
        if len(condition_parts) != 2:
            return {"error": f"Invalid join condition: {condition}. Expected format: table1.column = table2.column"}
            
        left_expr = condition_parts[0].strip()
        right_expr = condition_parts[1].strip()
        
        # Parse left_table.column and right_table.column formats
        if "." not in left_expr or "." not in right_expr:
            return {"error": f"Invalid join condition: {condition}. Expected format: table1.column = table2.column"}
            
        left_parts = left_expr.split(".")
        right_parts = right_expr.split(".")
        
        if len(left_parts) != 2 or len(right_parts) != 2:
            return {"error": f"Invalid join condition format. Expected table.column = table.column"}
            
        left_table_or_alias = left_parts[0].strip()
        left_column = left_parts[1].strip()
        right_table_or_alias = right_parts[0].strip()
        right_column = right_parts[1].strip()
        
        # Resolve aliases to actual table names
        left_actual_table = None
        right_actual_table = None
        
        if left_table_or_alias == main_table or left_table_or_alias == main_alias:
            left_actual_table = main_table
        elif left_table_or_alias == join_table or left_table_or_alias == join_alias:
            left_actual_table = join_table
        
        if right_table_or_alias == main_table or right_table_or_alias == main_alias:
            right_actual_table = main_table
        elif right_table_or_alias == join_table or right_table_or_alias == join_alias:
            right_actual_table = join_table
            
        if not left_actual_table:
            return {"error": f"Unknown table or alias '{left_table_or_alias}' in JOIN condition"}
        if not right_actual_table:
            return {"error": f"Unknown table or alias '{right_table_or_alias}' in JOIN condition"}
        
        # Check for column existence in tables
        main_table_schema = list(main_data[0].keys()) if main_data else []
        join_table_schema = list(join_data[0].keys()) if join_data else []
        
        if left_actual_table == main_table and left_column not in main_table_schema:
            return {"error": f"Column '{left_column}' not found in table '{main_table}'"}
        if right_actual_table == join_table and right_column not in join_table_schema:
            return {"error": f"Column '{right_column}' not found in table '{join_table}'"}
        if left_actual_table == join_table and left_column not in join_table_schema:
            return {"error": f"Column '{left_column}' not found in table '{join_table}'"}
        if right_actual_table == main_table and right_column not in main_table_schema:
            return {"error": f"Column '{right_column}' not found in table '{main_table}'"}
        
        # Extract WHERE clause (for additional filtering)
        where_clause = self._extract_where_clause(tokens)
        
        # Extract LIMIT clause to apply at the end (important for large joins)
        limit_offset = self._extract_limit_offset_clause(tokens)
        
        # If no explicit limit is provided, apply a safety limit for large joins
        if not limit_offset or limit_offset.get("limit") is None:
            # Default safety limit
            safety_limit = 100000  # 100K rows as default safety
            if not limit_offset:
                limit_offset = {"limit": safety_limit, "offset": 0}
            else:
                limit_offset["limit"] = safety_limit
                
        # Determine which table is which in the join
        if left_actual_table == main_table and right_actual_table == join_table:
            main_join_column = left_column
            other_join_column = right_column
            main_is_left = True
        elif left_actual_table == join_table and right_actual_table == main_table:
            main_join_column = right_column
            other_join_column = left_column
            main_is_left = False
        else:
            return {"error": f"Join condition references invalid tables: {left_actual_table}, {right_actual_table}"}
        
        # Check if we can use an index for the join
        join_uses_index = False
        join_indexable = True  # Assume it can be indexed
        
        # Check if indexes exist on join columns
        main_table_indexed = self._can_use_index(main_table, {"field": main_join_column, "operator": "=", "value": None})
        join_table_indexed = self._can_use_index(join_table, {"field": other_join_column, "operator": "=", "value": None})
        
        # Log warnings for non-indexed columns
        if not main_table_indexed:
            logger.warning(f"Performance warning: Column '{main_join_column}' in table '{main_table}' is not indexed. "
                          f"Consider creating an index: CREATE INDEX ON {main_table}({main_join_column})")
        
        if not join_table_indexed:
            logger.warning(f"Performance warning: Column '{other_join_column}' in table '{join_table}' is not indexed. "
                          f"Consider creating an index: CREATE INDEX ON {join_table}({other_join_column})")
        
        # Try to optimize with index if available
        if join_table_indexed:
            join_uses_index = True
            logger.info(f"Using index on {join_table}.{other_join_column} for JOIN query")
        elif main_table_indexed and join_type == "INNER JOIN":
            # If the index is on the main table, we can optimize by switching the join direction
            # But only for INNER JOIN, as order matters for other join types
            join_uses_index = True
            logger.info(f"Using index on {main_table}.{main_join_column} for JOIN query")
            # Swap tables for the join
            main_table, join_table = join_table, main_table
            main_data, join_data = join_data, main_data
            main_join_column, other_join_column = other_join_column, main_join_column
            main_is_left = not main_is_left
        
        # Performance optimization: Use set-based approach for FULL JOIN
        if join_type == "FULL JOIN":
            return self._execute_optimized_full_join(
                main_table, join_table, 
                main_data, join_data, 
                main_join_column, other_join_column, 
                main_is_left, where_clause, columns
            )
            
        # ENHANCED: Dynamic Join Strategy Selection
        # --------------------------------------
        # Choose the optimal join strategy based on data characteristics
        
        # Determine table sizes for decision making
        main_size = len(main_data)
        join_size = len(join_data)
        total_combinations = main_size * join_size
        size_ratio = max(main_size, join_size) / min(main_size, join_size) if min(main_size, join_size) > 0 else float('inf')
        
        # Analyze the join columns for cardinality
        # High cardinality = many unique values, low cardinality = few unique values
        main_unique_values = len(set(record.get(main_join_column) for record in main_data if record.get(main_join_column) is not None))
        join_unique_values = len(set(record.get(other_join_column) for record in join_data if record.get(other_join_column) is not None))
        
        main_cardinality_ratio = main_unique_values / main_size if main_size > 0 else 0
        join_cardinality_ratio = join_unique_values / join_size if join_size > 0 else 0
        
        # Log the join characteristics for debugging
        logger.debug(f"Join characteristics: main_size={main_size}, join_size={join_size}, size_ratio={size_ratio:.2f}")
        logger.debug(f"Cardinality: main={main_cardinality_ratio:.2f}, join={join_cardinality_ratio:.2f}")
        
        # Determine if we should use a hash join or nested loop join
        use_hash_join = True  # Default to hash join as it's generally faster
        
        # Override with auto-selection if enabled
        if self.join_strategy_preferences["auto"]:
            hash_join_threshold = self.join_strategy_preferences["hash_join_threshold"]
            size_ratio_threshold = self.join_strategy_preferences["size_ratio_threshold"]
            nested_loop_max = self.join_strategy_preferences["nested_loop_max"]
            
            # Use nested loop join when:
            # 1. Both tables are very small (below threshold)
            # 2. One table is indexed AND much smaller than the other
            # 3. Total size is manageable and one table is very small
            if (main_size < hash_join_threshold and join_size < hash_join_threshold):
                use_hash_join = False
                logger.debug("Using nested loop join: Both tables are small")
            elif ((main_table_indexed or join_table_indexed) and 
                  size_ratio > size_ratio_threshold and 
                  min(main_size, join_size) < hash_join_threshold):
                use_hash_join = False
                logger.debug("Using nested loop join: One table is indexed and much smaller")
            elif (max(main_size, join_size) < nested_loop_max and 
                  min(main_size, join_size) < hash_join_threshold / 10):
                use_hash_join = False
                logger.debug("Using nested loop join: Total size is manageable and one table is very small")
        
        # Log the chosen strategy
        if use_hash_join:
            logger.info(f"Using hash join strategy for {join_type} operation")
        else:
            logger.info(f"Using nested loop join strategy for {join_type} operation")
            
        # ENHANCED: Workload-Optimized Concurrency Selection
        # ------------------------------------------------
        # Determine if we should use parallel execution
        
        # Define thresholds for what constitutes a "large" join
        large_join_threshold = 50000  # Total possible combinations
        large_individual_table = 10000  # Rows in either table
        
        # Determine if we should use parallel execution
        use_parallel = (
            total_combinations > large_join_threshold or 
            len(main_data) > large_individual_table or 
            len(join_data) > large_individual_table
        )
        
        # Determine the type of parallel execution (processes vs threads)
        use_processes = False
        
        # Auto-detect workload type if enabled
        if self.concurrency_preferences["auto_detect_workload"]:
            # Check system load indicators to determine workload type
            # High CPU usage suggests CPU-bound workload
            # High disk I/O suggests I/O-bound workload
            
            # First attempt to make an educated guess about workload type
            is_io_bound = False
            
            # Consider join operations with large tables and low cardinality to be I/O bound
            # This means we're likely to read a lot of records with few matches
            if (max(main_size, join_size) > 50000 and 
                min(main_cardinality_ratio, join_cardinality_ratio) < 0.1):
                is_io_bound = True
                logger.debug("Workload classified as I/O-bound based on data characteristics")
            
            # Check system CPU usage if available
            try:
                cpu_usage = psutil.cpu_percent(interval=0.1)
                disk_io = psutil.disk_io_counters()
                
                if cpu_usage > 70:  # High CPU usage suggests CPU-bound
                    is_io_bound = False
                    logger.debug(f"Workload classified as CPU-bound based on high CPU usage ({cpu_usage}%)")
                
                # Use processes for CPU-bound work, threads for I/O-bound
                use_processes = not is_io_bound
                
            except (ImportError, AttributeError):
                # psutil not available or method not supported
                # Fall back to size-based heuristic
                very_large_threshold = 1000000  # 1M rows
                use_processes = total_combinations > very_large_threshold
        else:
            # Use the size-based heuristic as before
            very_large_threshold = 1000000  # 1M rows
            use_processes = total_combinations > very_large_threshold
            
        parallel_type = "process-based" if use_processes else "thread-based"
        if use_parallel:
            logger.info(f"Using {parallel_type} parallelism for {join_type} operation ({total_combinations} combinations)")
            
            # Build hash table for lookups (this part remains single-threaded)
            hash_table = {}
            
            if use_hash_join:
                # Build hash table from main table
                for record in main_data:
                    key = record.get(main_join_column)
                    if key not in hash_table:
                        hash_table[key] = []
                    hash_table[key].append(record)
                
                # Prepare for parallel execution - define the worker function
                def process_join_batch(batch):
                    batch_results = []
                    for record in batch:
                        key = record.get(other_join_column)
                        
                        if key in hash_table:
                            # Matching records found
                            for match in hash_table[key]:
                                # Create joined record
                                joined_record = {}
                                
                                # Add columns from both tables with appropriate prefixes
                                if main_is_left:
                                    # Main table is left
                                    for k, v in match.items():
                                        joined_record[f"{main_table}.{k}"] = v
                                    for k, v in record.items():
                                        joined_record[f"{join_table}.{k}"] = v
                                else:
                                    # Main table is right
                                    for k, v in record.items():
                                        joined_record[f"{left_actual_table}.{k}"] = v
                                    for k, v in match.items():
                                        joined_record[f"{right_actual_table}.{k}"] = v
                                
                                # Apply WHERE filter if needed
                                if where_clause:
                                    if self._record_matches_where(joined_record, where_clause):
                                        batch_results.append(joined_record)
                                else:
                                    batch_results.append(joined_record)
                        elif join_type == "RIGHT JOIN":
                            # No match but include in result for RIGHT JOIN
                            joined_record = {}
                            
                            # Add NULL values for main (left) table
                            for sample_record in main_data[:1] if main_data else [{}]:
                                for k in sample_record.keys():
                                    joined_record[f"{main_table}.{k}"] = None
                            
                            # Add values from join (right) table
                            for k, v in record.items():
                                joined_record[f"{join_table}.{k}"] = v
                            
                            # Apply WHERE filter if needed
                            if where_clause:
                                if self._record_matches_where(joined_record, where_clause):
                                    batch_results.append(joined_record)
                            else:
                                batch_results.append(joined_record)
                                
                    return batch_results
                
                # Split join_data into batches for parallel processing
                batch_size = max(100, len(join_data) // (os.cpu_count() or 4))
                batches = [join_data[i:i+batch_size] for i in range(0, len(join_data), batch_size)]
                
                # Process batches in parallel - Use either ProcessPoolExecutor or ThreadPoolExecutor
                result = []
                rows_processed = 0
                
                # Choose the appropriate executor based on join size
                if use_processes:
                    # Use process-based parallelism for CPU-intensive operations
                    logger.info("Using ProcessPoolExecutor for CPU-intensive JOIN operation")
                    with concurrent.futures.ProcessPoolExecutor() as executor:
                        # Create a generator that yields futures as they are submitted
                        # This allows us to process results in a streaming fashion
                        future_generator = (executor.submit(process_join_batch, batch) for batch in batches)
                        
                        # Process results as they complete to avoid memory buildup
                        for future in concurrent.futures.as_completed(list(future_generator)):
                            try:
                                batch_results = future.result()
                                # Apply streaming limit if specified
                                if limit_offset and limit_offset.get("limit"):
                                    # Check if we've hit the limit
                                    if rows_processed + len(batch_results) >= limit_offset["limit"]:
                                        # Only add records up to the limit
                                        remaining = limit_offset["limit"] - rows_processed
                                        result.extend(batch_results[:remaining])
                                        rows_processed += len(batch_results[:remaining])
                                        # Break early - we've hit our limit
                                        break
                                
                                # No limit or haven't reached it yet
                                result.extend(batch_results)
                                rows_processed += len(batch_results)
                            except Exception as exc:
                                logger.error(f"Batch processing generated an exception: {exc}")
                else:
                    # Use thread-based parallelism for I/O-bound operations
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future_to_batch = {executor.submit(process_join_batch, batch): batch for batch in batches}
                        
                        for future in concurrent.futures.as_completed(future_to_batch):
                            batch = future_to_batch[future]
                            try:
                                batch_results = future.result()
                                # Apply streaming limit if specified
                                if limit_offset and limit_offset.get("limit"):
                                    # Check if we've hit the limit
                                    if rows_processed + len(batch_results) >= limit_offset["limit"]:
                                        # Only add records up to the limit
                                        remaining = limit_offset["limit"] - rows_processed
                                        result.extend(batch_results[:remaining])
                                        rows_processed += len(batch_results[:remaining])
                                        # Break early - we've hit our limit
                                        break
                                
                                # No limit or haven't reached it yet
                                result.extend(batch_results)
                                rows_processed += len(batch_results)
                            except Exception as exc:
                                logger.error(f"Batch processing generated an exception: {exc}")
            else:
                # Build hash table from join table
                for record in join_data:
                    key = record.get(other_join_column)
                    if key not in hash_table:
                        hash_table[key] = []
                    hash_table[key].append(record)
                
                # Prepare for parallel execution - define the worker function
                def process_join_batch(batch):
                    batch_results = []
                    for record in batch:
                        key = record.get(main_join_column)
                        
                        if key in hash_table:
                            # Matching records found
                            for match in hash_table[key]:
                                # Create joined record
                                joined_record = {}
                                
                                # Add columns from both tables with appropriate prefixes
                                if main_is_left:
                                    # Add main table columns (left)
                                    for k, v in record.items():
                                        joined_record[f"{main_table}.{k}"] = v
                                    # Add join table columns (right)
                                    for k, v in match.items():
                                        joined_record[f"{join_table}.{k}"] = v
                                else:
                                    # Add join table columns (left)
                                    for k, v in match.items():
                                        joined_record[f"{left_actual_table}.{k}"] = v
                                    # Add main table columns (right)
                                    for k, v in record.items():
                                        joined_record[f"{right_actual_table}.{k}"] = v
                                
                                # Apply WHERE filter if needed
                                if where_clause:
                                    if self._record_matches_where(joined_record, where_clause):
                                        batch_results.append(joined_record)
                                else:
                                    batch_results.append(joined_record)
                        elif join_type == "LEFT JOIN":
                            # No match but include in result for LEFT JOIN
                            joined_record = {}
                            
                            # Add values from main (left) table
                            for k, v in record.items():
                                joined_record[f"{main_table}.{k}"] = v
                            
                            # Add NULL values for join (right) table
                            for sample_record in join_data[:1] if join_data else [{}]:
                                for k in sample_record.keys():
                                    joined_record[f"{join_table}.{k}"] = None
                            
                            # Apply WHERE filter if needed
                            if where_clause:
                                if self._record_matches_where(joined_record, where_clause):
                                    batch_results.append(joined_record)
                            else:
                                batch_results.append(joined_record)
                                
                    return batch_results
                
                # Split main_data into batches for parallel processing
                batch_size = max(100, len(main_data) // (os.cpu_count() or 4))
                batches = [main_data[i:i+batch_size] for i in range(0, len(main_data), batch_size)]
                
                # Process batches in parallel
                result = []
                rows_processed = 0
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_to_batch = {executor.submit(process_join_batch, batch): batch for batch in batches}
                    for future in concurrent.futures.as_completed(future_to_batch):
                        batch = future_to_batch[future]
                        rows_processed += len(batch)
                        try:
                            batch_results = future.result()
                            result.extend(batch_results)
                        except Exception as exc:
                            logger.error(f"Batch processing generated an exception: {exc}")
        else:
            # Regular non-parallel execution for smaller joins
            # Build hash table for lookups
            hash_table = {}
            if use_hash_join:
                # Build hash table from main table
                for record in main_data:
                    key = record.get(main_join_column)
                    if key not in hash_table:
                        hash_table[key] = []
                    hash_table[key].append(record)
                
                # Perform the JOIN - probe with join table
                result = []
                rows_processed = 0
                
                for record in join_data:
                    rows_processed += 1
                    key = record.get(other_join_column)
                    
                    if key in hash_table:
                        # Matching records found
                        for match in hash_table[key]:
                            # Create joined record
                            joined_record = {}
                            
                            # Add columns from both tables with appropriate prefixes
                            if main_is_left:
                                # Main table is left
                                for k, v in match.items():
                                    joined_record[f"{main_table}.{k}"] = v
                                for k, v in record.items():
                                    joined_record[f"{join_table}.{k}"] = v
                            else:
                                # Main table is right
                                for k, v in record.items():
                                    joined_record[f"{left_actual_table}.{k}"] = v
                                for k, v in match.items():
                                    joined_record[f"{right_actual_table}.{k}"] = v
                            
                            # Apply WHERE filter if needed
                            if where_clause:
                                if self._record_matches_where(joined_record, where_clause):
                                    result.append(joined_record)
                            else:
                                result.append(joined_record)
                    elif join_type == "RIGHT JOIN":
                        # No match but include in result for RIGHT JOIN
                        joined_record = {}
                        
                        # Add NULL values for main (left) table
                        for sample_record in main_data[:1] if main_data else [{}]:
                            for k in sample_record.keys():
                                joined_record[f"{main_table}.{k}"] = None
                        
                        # Add values from join (right) table
                        for k, v in record.items():
                            joined_record[f"{join_table}.{k}"] = v
                        
                        # Apply WHERE filter if needed
                        if where_clause:
                            if self._record_matches_where(joined_record, where_clause):
                                result.append(joined_record)
                        else:
                            result.append(joined_record)
            else:
                # Build hash table from join table
                for record in join_data:
                    key = record.get(other_join_column)
                    if key not in hash_table:
                        hash_table[key] = []
                    hash_table[key].append(record)
                
                # Perform the JOIN - probe with main table
                result = []
                rows_processed = 0
                
                for record in main_data:
                    rows_processed += 1
                    key = record.get(main_join_column)
                    
                    if key in hash_table:
                        # Matching records found
                        for match in hash_table[key]:
                            # Create joined record
                            joined_record = {}
                            
                            # Add columns from both tables with appropriate prefixes
                            if main_is_left:
                                # Add main table columns (left)
                                for k, v in record.items():
                                    joined_record[f"{main_table}.{k}"] = v
                                # Add join table columns (right)
                                for k, v in match.items():
                                    joined_record[f"{join_table}.{k}"] = v
                            else:
                                # Add join table columns (left)
                                for k, v in match.items():
                                    joined_record[f"{left_actual_table}.{k}"] = v
                                # Add main table columns (right)
                                for k, v in record.items():
                                    joined_record[f"{right_actual_table}.{k}"] = v
                            
                            # Apply WHERE filter if needed
                            if where_clause:
                                if self._record_matches_where(joined_record, where_clause):
                                    result.append(joined_record)
                            else:
                                result.append(joined_record)
                    elif join_type == "LEFT JOIN":
                        # No match but include in result for LEFT JOIN
                        joined_record = {}
                        
                        # Add values from main (left) table
                        for k, v in record.items():
                            joined_record[f"{main_table}.{k}"] = v
                        
                        # Add NULL values for join (right) table
                        for sample_record in join_data[:1] if join_data else [{}]:
                            for k in sample_record.keys():
                                joined_record[f"{join_table}.{k}"] = None
                        
                        # Apply WHERE filter if needed
                        if where_clause:
                            if self._record_matches_where(joined_record, where_clause):
                                result.append(joined_record)
                        else:
                            result.append(joined_record)
        
        # Apply column projection if needed
        if columns and "*" not in columns:
            # Handle table-prefixed column names
            detailed_columns = []
            for col in columns:
                if "." in col:
                    detailed_columns.append(col)
                else:
                    # If no prefix given, try both tables
                    found = False
                    for prefix in [f"{main_table}.", f"{join_table}."]:
                        prefixed_col = f"{prefix}{col}"
                        if any(prefixed_col in rec for rec in result[:1]):
                            detailed_columns.append(prefixed_col)
                            found = True
                            break
                    if not found and result:
                        # If not found with prefix, keep original
                        detailed_columns.append(col)
                    
            result = self._project_columns(result, detailed_columns)
        
        # Calculate and add execution time directly in _execute_join_query
        join_execution_time = (time.time() - join_start_time) * 1000
        
        # Log explicit warning for slow JOIN queries with graduated severity levels
        if join_execution_time > self.very_slow_query_threshold:
            # Critical slow JOIN query
            warning_msg = (
                f"⚠️ CRITICAL: Slow {join_type} query ({join_execution_time:.2f}ms) between '{main_table}' and '{join_table}'. " + 
                f"Rows processed: {rows_processed}, Rows returned: {len(result)}. "
            )
            
            if not main_table_indexed or not join_table_indexed:
                warning_msg += (
                    f"OPTIMIZE: Create indexes on {left_actual_table}.{left_column} and {right_actual_table}.{right_column}. " +
                    f"Consider implementing a query plan with EXPLAIN ANALYZE."
                )
            
            logger.warning(warning_msg)
            self.slow_query_logger.warning(warning_msg)
        
        elif join_execution_time > self.slow_query_threshold:
            # Moderate slow JOIN query
            warning_msg = (
                f"⚠️ WARNING: Slow {join_type} query ({join_execution_time:.2f}ms) joining '{main_table}' and '{join_table}'. " + 
                f"Rows processed: {rows_processed}, Rows returned: {len(result)}. "
            )
            
            if not main_table_indexed or not join_table_indexed:
                warning_msg += f"JOIN performance could be improved by creating indexes on " + \
                               f"{left_actual_table}.{left_column} and {right_actual_table}.{right_column}."
            
            logger.warning(warning_msg)
            self.slow_query_logger.warning(warning_msg)
        
        # Add performance metadata to result
        response = {"result": result, "message": f"{len(result)} records retrieved via {join_type} operation"}
        response["join_performance"] = {
            "execution_time_ms": round(join_execution_time, 2),
            "rows_processed": rows_processed,
            "rows_returned": len(result),
            "join_type": join_type,
            "used_index": join_uses_index,
            "used_parallel": use_parallel if 'use_parallel' in locals() else False,
            "main_table_indexed": main_table_indexed,
            "join_table_indexed": join_table_indexed,
            "query_plan": self._generate_query_plan_hint(main_table, join_table, join_uses_index)
        }
        
        # Add index suggestions if needed
        if not main_table_indexed or not join_table_indexed:
            response["join_performance"]["index_suggestions"] = []
            if not main_table_indexed:
                response["join_performance"]["index_suggestions"].append(
                    f"CREATE INDEX ON {main_table}({main_join_column})"
                )
            if not join_table_indexed:
                response["join_performance"]["index_suggestions"].append(
                    f"CREATE INDEX ON {join_table}({other_join_column})"
                )
        
        return response

    def _execute_optimized_full_join(self, main_table, join_table, main_data, join_data, 
                                     main_join_column, other_join_column, main_is_left, 
                                     where_clause, columns, main_prefix=None, join_prefix=None):
        """Execute an optimized FULL JOIN using set operations."""
        # Use provided prefixes or default to table names
        main_prefix = main_prefix or main_table
        join_prefix = join_prefix or join_table
        
        # Start with empty result set
        result = []
        
        # Track the keys that have been processed
        processed_keys = set()
        
        # Extract LIMIT for early termination
        limit_offset = None
        if columns and 'tokens' in columns:
            limit_offset = self._extract_limit_offset_clause(columns['tokens'])
        
        # Apply a safety limit if not specified
        if not limit_offset or limit_offset.get("limit") is None:
            limit_limit = 100000  # 100K rows safety limit
            if not limit_offset:
                limit_offset = {"limit": limit_limit, "offset": 0}
            else:
                limit_offset["limit"] = limit_limit
                
        limit = limit_offset.get("limit", float('inf'))
        offset = limit_offset.get("offset", 0)
        
        # Flag to indicate partial results
        partial_result = False
        
        # Process matches in batches to avoid memory issues with large datasets
        batch_size = 1000
        main_batches = [main_data[i:i+batch_size] for i in range(0, len(main_data), batch_size)]
        
        # Process each batch of the main table
        for main_batch in main_batches:
            # Check if we've already hit the limit
            if len(result) >= limit + offset:
                partial_result = True
                break
                
            # Build a hash table for the current batch
            batch_hash = {}
            for record in main_batch:
                key = record.get(main_join_column)
                if key not in batch_hash:
                    batch_hash[key] = []
                batch_hash[key].append(record)
                processed_keys.add(key)
            
            # Find matches in the join table
            for join_record in join_data:
                join_key = join_record.get(other_join_column)
                
                if join_key in batch_hash:
                    # We have matching records
                    for main_record in batch_hash[join_key]:
                        # Create joined record
                        joined_record = {}
                        
                        # Add prefixed columns from both tables
                        for k, v in main_record.items():
                            joined_record[f"{main_prefix}.{k}"] = v
                        for k, v in join_record.items():
                            joined_record[f"{join_prefix}.{k}"] = v
                        
                        # Apply WHERE filter if needed
                        if where_clause:
                            if self._record_matches_where(joined_record, where_clause):
                                result.append(joined_record)
                                # Check if we've hit the limit after adding this record
                                if len(result) >= limit + offset:
                                    partial_result = True
                                    break
                        else:
                            result.append(joined_record)
                            # Check if we've hit the limit after adding this record
                            if len(result) >= limit + offset:
                                partial_result = True
                                break
                
                # Break out of the loop if we've hit the limit
                if partial_result:
                    break
            
            # Break out of the batch loop if we've hit the limit
            if partial_result:
                break
        
        # Now process records from join_table that don't have matches in main_table
        # Only if we haven't hit the limit yet
        if not partial_result:
            # Process in batches to manage memory
            join_batches = [join_data[i:i+batch_size] for i in range(0, len(join_data), batch_size)]
            
            for join_batch in join_batches:
                # Check if we've hit the limit
                if len(result) >= limit + offset:
                    partial_result = True
                    break
                    
                for join_record in join_batch:
                    join_key = join_record.get(other_join_column)
                    
                    if join_key not in processed_keys:
                        # No matching record in main_table
                        joined_record = {}
                        
                        # Add NULL values for main table
                        for sample_record in main_data[:1] if main_data else [{}]:
                            for k in sample_record.keys():
                                joined_record[f"{main_prefix}.{k}"] = None
                        
                        # Add values from join table
                        for k, v in join_record.items():
                            joined_record[f"{join_prefix}.{k}"] = v
                        
                        # Apply WHERE filter if needed
                        if where_clause:
                            if self._record_matches_where(joined_record, where_clause):
                                result.append(joined_record)
                                # Check if we've hit the limit
                                if len(result) >= limit + offset:
                                    partial_result = True
                                    break
                        else:
                            result.append(joined_record)
                            # Check if we've hit the limit
                            if len(result) >= limit + offset:
                                partial_result = True
                                break
                
                # Break out of the batch loop if we've hit the limit
                if partial_result:
                    break
        
        # Apply offset if specified
        if offset > 0:
            result = result[offset:]
            
        # Apply ORDER BY if needed
        order_by = None
        if columns and 'tokens' in columns:
            order_by = self._extract_order_by_clause(columns['tokens'])
        
        if order_by:
            result = self._sort_data(result, order_by)
            
        # Create the final result with appropriate message
        message = f"{len(result)} records retrieved from FULL JOIN"
        if partial_result:
            message += f" (limited to {limit} rows)"
            
        final_result = {
            "result": result,
            "message": message
        }
        
        # Add partial result info if applicable
        if partial_result:
            final_result["partial_result"] = True
            final_result["limit_applied"] = limit
            
        return final_result
        
    def _execute_cross_join(self, main_table, join_table, main_data, join_data, tokens, columns=None):
        """Execute a CROSS JOIN (cartesian product) between two tables."""
        start_time = time.time()
        
        # Extract WHERE clause for additional filtering
        where_clause = self._extract_where_clause(tokens)
        
        # Define what makes a small table - this can be adjusted as needed
        small_table_threshold = 100  # tables with fewer than 100 rows are considered "small"
        
        # Check if this might be an accidental CROSS JOIN by analyzing WHERE clause
        if where_clause:
            field = where_clause.get("field", "")
            # Look for potential JOIN conditions in WHERE
            if "." in field and where_clause.get("operator") == "=":
                value = where_clause.get("value", "")
                if isinstance(value, str) and "." in value:
                    # This looks like a JOIN condition mistakenly used in WHERE
                    logger.warning(f"⚠️ INEFFICIENT QUERY DETECTED: CROSS JOIN with filtering condition in WHERE "
                                  f"that looks like a JOIN condition: {field} = {value}")
                    logger.warning(f"💡 SUGGESTION: Replace 'CROSS JOIN {join_table} WHERE {field} = {value}' "
                                  f"with 'INNER JOIN {join_table} ON {field} = {value}' for better performance")
        
        # Perform the CROSS JOIN (cartesian product)
        result = []
        rows_processed = 0
        
        # Calculate total combinations
        total_combinations = len(main_data) * len(join_data)
        
        # Check if both tables are small enough to skip warnings
        both_tables_small = (len(main_data) <= small_table_threshold and len(join_data) <= small_table_threshold)
        
        
        # Only warn for large cross joins if both tables aren't small
        if total_combinations > 10000 and not both_tables_small:
            logger.warning(f"⚠️ PERFORMANCE WARNING: Large CROSS JOIN with {total_combinations} combinations between '{main_table}' ({len(main_data)} rows) and '{join_table}' ({len(join_data)} rows)")
            logger.warning(f"💡 OPTIMIZATION TIP: Consider using INNER JOIN with an ON condition instead of CROSS JOIN to reduce the result size.")
        
        # For very large tables, provide a specific warning
        if len(main_data) > 1000 and len(join_data) > 1000:
            logger.warning(f"⚠️ CRITICAL: CROSS JOIN between two large tables ({len(main_data)} x {len(join_data)} rows) may cause performance issues or memory errors.")
            logger.warning(f"💡 OPTIMIZATION TIP: Consider adding LIMIT clause or using a more specific JOIN type with conditions.")
        
        # Decide whether to use parallel processing for large cross joins
        use_parallel = not both_tables_small and total_combinations > 100000
        
        # Batch processing for very large cross joins to avoid memory issues
        batch_size = 5000
        need_batching = total_combinations > 100000 and not use_parallel
        
        if use_parallel:
            # Use parallel processing for large cross joins
            logger.info(f"Using parallel processing for large CROSS JOIN (total combinations: {total_combinations})")
            
            def process_cross_join_batch(batch):
                batch_results = []
                for left_record in batch:
                    for right_record in join_data:
                        joined_record = {}
                        
                        # Add prefixed columns from both tables to avoid ambiguity
                        for k, v in left_record.items():
                            joined_record[f"{main_table}.{k}"] = v
                        for k, v in right_record.items():
                            joined_record[f"{join_table}.{k}"] = v
                        
                        # Apply additional WHERE clause filtering if needed
                        if where_clause:
                            if self._record_matches_where(joined_record, where_clause):
                                batch_results.append(joined_record)
                        else:
                            batch_results.append(joined_record)
                            
                return batch_results
            
            # Split main_data into batches for parallel processing
            cpu_count = os.cpu_count() or 4
            optimal_batch_size = max(10, len(main_data) // cpu_count)
            batches = [main_data[i:i+optimal_batch_size] for i in range(0, len(main_data), optimal_batch_size)]
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_batch = {executor.submit(process_cross_join_batch, batch): batch for batch in batches}
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    rows_processed += len(batch) * len(join_data)
                    try:
                        batch_results = future.result()
                        result.extend(batch_results)
                    except Exception as exc:
                        logger.error(f"Batch processing generated an exception: {exc}")
        
        elif need_batching:
            logger.info(f"Using batch processing for large CROSS JOIN (total combinations: {total_combinations})")
            
            # Process in batches
            for i in range(0, len(main_data), batch_size):
                batch_end = min(i + batch_size, len(main_data))
                main_batch = main_data[i:batch_end]
                
                # Calculate cartesian product for this batch
                for left_record in main_batch:
                    for right_record in join_data:
                        rows_processed += 1
                        joined_record = {}
                        
                        # Add prefixed columns from both tables to avoid ambiguity
                        for k, v in left_record.items():
                            joined_record[f"{main_table}.{k}"] = v
                        for k, v in right_record.items():
                            joined_record[f"{join_table}.{k}"] = v
                        
                        # Apply additional WHERE clause filtering if needed
                        if where_clause:
                            if self._record_matches_where(joined_record, where_clause):
                                result.append(joined_record)
                        else:
                            result.append(joined_record)
        else:
            # Standard processing for smaller joins
            for left_record in main_data:
                for right_record in join_data:
                    rows_processed += 1
                    joined_record = {}
                    
                    # Add prefixed columns from both tables to avoid ambiguity
                    for k, v in left_record.items():
                        joined_record[f"{main_table}.{k}"] = v
                    for k, v in right_record.items():
                        joined_record[f"{join_table}.{k}"] = v
                    
                    # Apply additional WHERE clause filtering if needed
                    if where_clause:
                        if self._record_matches_where(joined_record, where_clause):
                            result.append(joined_record)
                    else:
                        result.append(joined_record)
        
        # Apply column projection if needed
        if columns and "*" not in columns:
            detailed_columns = []
            for col in columns:
                if "." in col:
                    detailed_columns.append(col)
                else:
                    # If no prefix given, try both tables
                    found = False
                    for prefix in [f"{main_table}.", f"{join_table}."]:
                        prefixed_col = f"{prefix}{col}"
                        if any(prefixed_col in rec for rec in result[:1]):
                            detailed_columns.append(prefixed_col)
                            found = True
                            break
                    if not found and result:
                        # If not found with prefix, keep original
                        detailed_columns.append(col)
                    
            result = self._project_columns(result, detailed_columns)
        
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000
        
        # Only log warning for slow cross joins if they aren't small
        if execution_time > self.very_slow_query_threshold and not both_tables_small:
            warning_msg = (
                f"⚠️ CRITICAL: Slow CROSS JOIN ({execution_time:.2f}ms) between '{main_table}' and '{join_table}'. " + 
                f"Total combinations: {total_combinations}, Rows returned: {len(result)}. " +
                f"Consider using a more specific JOIN type with ON conditions instead."
            )
            logger.warning(warning_msg)
            self.slow_query_logger.warning(warning_msg)
            
            # Add specific optimization tips for CROSS JOIN
            optimization_msg = (
                f"💡 CROSS JOIN OPTIMIZATION TIPS:\n" +
                f"1. Replace with INNER JOIN using a suitable join condition\n" +
                f"2. Add a LIMIT clause to restrict the result set\n" +
                f"3. Filter tables first with WHERE before joining\n" +
                f"4. Consider creating a materialized view if this join is used frequently"
            )
            logger.warning(optimization_msg)
            self.slow_query_logger.warning(optimization_msg)
        
        # Add performance metadata to result
        response = {"result": result, "message": f"{len(result)} records retrieved via CROSS JOIN operation"}
        response["join_performance"] = {
            "execution_time_ms": round(execution_time, 2),
            "total_combinations": total_combinations,
            "rows_returned": len(result),
            "join_type": "CROSS JOIN",
            "batched_processing": need_batching,
            "parallel_processing": use_parallel if 'use_parallel' in locals() else False,
            "small_tables_optimization": both_tables_small
        }
        
        # Add optimization tips for large result sets if not using small tables
        if len(result) > 1000 and not both_tables_small:
            response["join_performance"]["optimization_tips"] = [
                "Replace CROSS JOIN with INNER JOIN using a specific condition",
                "Add a WHERE clause to filter the results",
                "Use LIMIT to restrict the result set size"
            ]
        
        return response

    def _generate_query_plan_hint(self, main_table, join_table, used_index):
        """Generate a simplified query plan hint to help understand query execution."""
        if used_index:
            return f"Index Scan on {join_table} → Hash Join with {main_table}"
        else:
            return f"Sequential Scan on {join_table} → Hash Join with {main_table}"

    def _generate_detailed_optimization_tips(self, query, execution_time, table_name, where_clause, limit_offset, result_count):
        """Generate detailed optimization tips for slow queries with specific suggestions."""
        tips = []
        
        # Check for missing LIMIT on large result sets
        if result_count > 100 and not limit_offset:
            tips.append(f"Add LIMIT clause to reduce the {result_count} rows returned")
        
        # Check for missing indexes on WHERE clause
        if where_clause and where_clause["operator"] in self._indexable_operators:
            field = where_clause["field"]
            if not self._can_use_index(table_name, where_clause):
                tips.append(f"Create an index on {table_name}.{field} with: CREATE INDEX ON {table_name}({field})")
        
        # Check for query pattern
        command = query.get("command", "")
        if command == "SELECT":
            tokens = query.get("tokens", [])
            
            # Check if this might be an analytics query
            if "COUNT" in str(tokens) or "SUM" in str(tokens) or "AVG" in str(tokens):
                tips.append("For analytics queries, consider creating materialized views or pre-aggregated tables")
            
            # Check if all columns are selected
            columns = self._extract_columns(tokens)
            if "*" in columns:
                tips.append("Specify only needed columns instead of using SELECT * to reduce data transfer")
        
        # Suggest EXPLAIN ANALYZE for very slow queries
        if execution_time > self.very_slow_query_threshold:
            tips.append("Consider using EXPLAIN ANALYZE to debug the query execution plan")
        
        # Suggest caching for repeated queries
        tips.append("Use query caching for frequently executed identical queries")
        
        return tips

    def configure_performance_thresholds(self, slow_threshold=None, very_slow_threshold=None):
        """Configure the thresholds for slow query warnings.
        
        Args:
            slow_threshold: Warning threshold in milliseconds for slow queries
            very_slow_threshold: Warning threshold in milliseconds for very slow queries
        """
        if slow_threshold is not None:
            self.slow_query_threshold = slow_threshold
            logger.info(f"Slow query threshold configured to {slow_threshold}ms")
            
        if very_slow_threshold is not None:
            self.very_slow_query_threshold = very_slow_threshold
            logger.info(f"Very slow query threshold configured to {very_slow_threshold}ms")
            
        return {
            "slow_query_threshold": self.slow_query_threshold,
            "very_slow_query_threshold": self.very_slow_query_threshold
        }

    def get_slow_query_log_path(self):
        """Return the path to the slow query log file."""
        return "logs/slow_queries.log"

    def _track_slow_query(self, query, execution_time):
        """Track a slow query for reporting."""
        slow_query_info = {
            "query": query.get("original_query", "Unknown query"),
            "execution_time_ms": round(execution_time, 2),
            "timestamp": time.time(),
            "command": query.get("command"),
            "optimization_tips": self._generate_optimization_tips(query, execution_time)
        }
        
        # Add to recent slow queries, maintaining max size
        self.recent_slow_queries.append(slow_query_info)
        if len(self.recent_slow_queries) > self.max_slow_queries_history:
            self.recent_slow_queries.pop(0)  # Remove oldest
            
    def _generate_optimization_tips(self, query, execution_time):
        """Generate optimization tips based on the query and its execution time."""
        command = query.get("command", "")
        tokens = query.get("tokens", [])
        tips = []
        
        if command == "SELECT":
            # Check for possible indexing opportunities
            where_clause = self._extract_where_clause(tokens)
            if where_clause:
                field = where_clause.get("field", "")
                operator = where_clause.get("operator", "")
                
                from engine.sql_parser import SQLParser
                parser = SQLParser()
                table_name = parser.extract_table_name(command, tokens)
                
                if operator in self._indexable_operators and not self._can_use_index(table_name, where_clause):
                    tips.append(f"Create an index on {table_name}.{field} to speed up this query.")
                    
            # Check for JOIN operations
            join_clause = self._extract_join_clause(tokens)
            if join_clause:
                main_table = parser.extract_table_name(command, tokens)
                join_table = join_clause.get("table", "")
                left_table = join_clause.get("left_table", "")
                left_column = join_clause.get("left_column", "")
                right_table = join_clause.get("right_table", "")
                right_column = join_clause.get("right_column", "")
                
                tips.append(f"Consider creating indexes on the join columns: {left_table}.{left_column} and {right_table}.{right_column}")
                
            # Check for missing LIMIT clause
            if "LIMIT" not in [t.upper() if isinstance(t, str) else t for t in tokens]:
                tips.append("Add a LIMIT clause to restrict the number of rows returned.")
                
        elif command == "UPDATE" or command == "DELETE":
            # Check for missing WHERE clause
            where_clause = self._extract_where_clause(tokens)
            if not where_clause:
                tips.append(f"This {command} operation affects all rows. Consider adding a WHERE clause to target specific rows.")
            elif where_clause.get("operator") in self._indexable_operators:
                from engine.sql_parser import SQLParser
                parser = SQLParser()
                table_name = parser.extract_table_name(command, tokens)
                field = where_clause.get("field", "")
                
                if not self._can_use_index(table_name, where_clause):
                    tips.append(f"Create an index on {table_name}.{field} to speed up this operation.")
        
        # Add general tips
        if execution_time > self.very_slow_query_threshold:
            tips.append("Consider using the query cache for frequently executed identical queries.")
            
        # If no specific tips, add generic advice
        if not tips:
            tips.append("Analyze the query structure to identify potential bottlenecks.")
            
        return tips
        
    def get_performance_report(self):
        """Generate a comprehensive performance report."""
        report = {
            "query_stats": self.query_stats.copy(),
            "cache_stats": self.get_cache_stats(),
            "slow_queries": self.recent_slow_queries[:],
            "recommendations": []
        }
        
        # Generate overall recommendations
        if self.query_stats["table_scans"] > 0:
            scan_percentage = (self.query_stats["table_scans"] / max(1, self.query_stats["total_queries"])) * 100
            if scan_percentage > 20:  # If more than 20% of queries use full table scans
                report["recommendations"].append(
                    "High percentage of full table scans detected. Consider adding indexes to frequently queried columns."
                )
                
        if self.query_stats["slow_queries"] > 0:
            slow_percentage = (self.query_stats["slow_queries"] / max(1, self.query_stats["total_queries"])) * 100
            if slow_percentage > 10:  # If more than 10% of queries are slow
                report["recommendations"].append(
                    "High percentage of slow queries detected. Review the slow queries list for specific optimization opportunities."
                )
                
        # Check cache efficiency
        cache_stats = self.get_cache_stats()
        if "hit_ratio" in cache_stats:
            hit_ratio = float(cache_stats["hit_ratio"].replace("%", ""))
            if hit_ratio < 30 and self.cache_hits + self.cache_misses > 100:  # Low hit ratio with sufficient queries
                report["recommendations"].append(
                    "Low cache hit ratio. Consider increasing cache size or reviewing query patterns."
                )
                
        return report
