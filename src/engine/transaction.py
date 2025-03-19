import json
import os
import uuid
import time
import functools
from datetime import datetime
import random
import logging

# Setup logger for transaction module
logger = logging.getLogger('ProSquareSQL.Transaction')

# Configure logging if not already configured
if not logger.handlers:
    # Default configuration if not already set up elsewhere
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Create a handler for console output (we'll let the parent logger handle file output)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Add the handler to our logger
    logger.addHandler(console_handler)
    
    # Set default level - applications can override this
    logger.setLevel(logging.INFO)

class TransactionManager:
    def __init__(self):
        """Initialize the transaction manager with recovery capability."""
        self.transactions = {}
        self.active_transaction = None
        self.transaction_log = "data/transaction_log.json"
        self.pending_transactions_path = "data/pending_transactions"
        
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(self.transaction_log), exist_ok=True)
        
        # Operation handlers map operation types to their handler methods
        self.operation_handlers = {}
        
        # Register the built-in operation handlers
        self._register_default_handlers()
        
        # Create pending transactions directory
        os.makedirs(self.pending_transactions_path, exist_ok=True)
        
        # Auto-recover pending transactions on startup
        self.auto_recover_transactions()
        
        logger.debug("TransactionManager initialized")

    def _register_default_handlers(self):
        """Register the default operation handlers."""
        # Register built-in handlers
        self.register_operation_handler("save", self._apply_save_operation)
        self.register_operation_handler("delete", self._apply_delete_operation)
        
    def register_operation_handler(self, operation_type, handler_function):
        """Register a new operation handler."""
        self.operation_handlers[operation_type] = handler_function
        logger.debug(f"Registered operation handler for: {operation_type}")
        return handler_function  # Return the function for use as a decorator
        
    def operation_handler(self, operation_type):
        """Decorator to register an operation handler."""
        def decorator(func):
            self.register_operation_handler(operation_type, func)
            return func
        return decorator

    def begin_transaction(self):
        """Begin a new transaction with enhanced logging."""
        if self.active_transaction:
            logger.warning(f"Transaction already in progress: {self.active_transaction}")
            return f"Transaction already in progress: {self.active_transaction}"
        
        # Generate transaction ID
        transaction_id = f"TX-{int(time.time())}-{random.randint(1000, 9999)}"
        
        # Initialize transaction data
        self.transactions[transaction_id] = {
            "operations": [],
            "snapshots": {},
            "start_time": time.time(),
            "affected_tables": set()
        }
        
        self.active_transaction = transaction_id
        
        # Create a pending transaction file for crash recovery
        self._save_pending_transaction(transaction_id)
        
        logger.info(f"Started transaction: {transaction_id}")
        return f"Started transaction: {transaction_id}"

    def _save_pending_transaction(self, transaction_id):
        """Save a pending transaction to disk for crash recovery."""
        if transaction_id not in self.transactions:
            return
        
        # Create a transaction state file
        pending_file = os.path.join(self.pending_transactions_path, f"{transaction_id}.json")
        
        try:
            with open(pending_file, "w") as f:
                # Store minimal transaction data for recovery
                tx_data = self.transactions[transaction_id]
                recovery_data = {
                    "id": transaction_id,
                    "start_time": tx_data["start_time"],
                    "operations": tx_data["operations"],
                    "status": "pending"
                }
                json.dump(recovery_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save pending transaction {transaction_id}: {str(e)}")

    def log_operation(self, operation):
        """Log an operation with enhanced recovery support."""
        if not self.active_transaction:
            logger.warning("Attempted to log operation outside transaction")
            return "No active transaction."
        
        # Capture table snapshot as before
        if "operation" in operation:
            if operation["operation"] == "save" and "table_name" in operation:
                self._capture_table_snapshot(operation["table_name"])
            elif operation["operation"] == "delete" and "table_name" in operation:
                self._capture_table_snapshot(operation["table_name"])
        
        # Track affected tables
        if "table_name" in operation:
            self.transactions[self.active_transaction]["affected_tables"].add(operation["table_name"])
                
        # Add operation to the list
        self.transactions[self.active_transaction]["operations"].append(operation)
        
        # Update the pending transaction file with the new operation
        self._save_pending_transaction(self.active_transaction)

    def _capture_table_snapshot(self, table_name):
        """Capture a snapshot of the table for rollback purposes."""
        if not self.active_transaction:
            return
            
        # Only capture the first time we modify this table in the transaction
        if table_name in self.transactions[self.active_transaction]["snapshots"]:
            return
            
        # Get the current state of the table
        from engine.storage import Storage
        storage = Storage()
        
        if storage.table_exists(table_name):
            current_data = storage.get_table(table_name)
            self.transactions[self.active_transaction]["snapshots"][table_name] = current_data
            logger.debug(f"Captured snapshot of table {table_name} for transaction {self.active_transaction}")
        else:
            # Table doesn't exist yet, mark it as null for potential rollback
            self.transactions[self.active_transaction]["snapshots"][table_name] = None
            logger.debug(f"Table {table_name} doesn't exist, snapshot marked as None")

    def commit_transaction(self, debug_mode=False):
        """Commit transaction with cleanup of recovery files."""
        if not self.active_transaction:
            return "No active transaction to commit."
        
        transaction_id = self.active_transaction
        tx_data = self.transactions[transaction_id]
        
        # Start timer for overall commit process
        commit_start_time = time.time()
        
        # Setup performance tracking
        operation_times = []
        slow_operations = []
        SLOW_OPERATION_THRESHOLD = 500  # ms
        
        # Track statistics for reporting
        stats = {
            "operations_processed": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "creates": 0,
            "drops": 0,
            "other": 0,
            "errors": []
        }
        
        # Import here to avoid circular imports
        from engine.storage import Storage
        storage = Storage()
        
        # Debug output with more context
        if debug_mode:
            logger.debug(f"[DEBUG] Beginning commit of transaction {transaction_id}")
            logger.debug(f"[DEBUG] Transaction contains {len(tx_data['operations'])} operations")
            
            # Group operations by type for better visibility
            op_types = {}
            for op in tx_data["operations"]:
                cmd = op["command"].upper()
                if cmd not in op_types:
                    op_types[cmd] = 0
                op_types[cmd] += 1
            
            # Show operation summary
            logger.debug(f"[DEBUG] Operation types: {', '.join([f'{k}:{v}' for k,v in op_types.items()])}")
            logger.debug(f"[DEBUG] Affected tables: {', '.join(tx_data['affected_tables'])}")
        
        # Track dependencies between operations
        operation_dependencies = {}
        
        # Process all operations in order with enhanced tracking
        for operation_idx, operation in enumerate(tx_data["operations"]):
            command = operation["command"].upper()
            
            if debug_mode:
                step_description = f"Step {operation_idx+1}/{len(tx_data['operations'])}: {command}"
                
                # Add table info if available
                if "table_name" in operation:
                    step_description += f" on {operation['table_name']}"
                    
                # Add hint about dependencies
                if operation_idx > 0 and "table_name" in operation:
                    # Find previous operations on same table
                    for prev_idx, prev_op in enumerate(tx_data["operations"][:operation_idx]):
                        if "table_name" in prev_op and prev_op["table_name"] == operation["table_name"]:
                            if operation_idx not in operation_dependencies:
                                operation_dependencies[operation_idx] = []
                            operation_dependencies[operation_idx].append(prev_idx)
                
                logger.debug(f"[DEBUG] {step_description}")
                
                # Show dependencies if they exist
                if operation_idx in operation_dependencies:
                    deps = operation_dependencies[operation_idx]
                    logger.debug(f"[DEBUG]   Depends on steps: {', '.join([str(d+1) for d in deps])}")
            
            operation_start_time = time.time()
            
            try:
                self._process_operation(operation, stats, storage, debug_mode)
                
                # Track operation performance
                op_time = int((time.time() - operation_start_time) * 1000)  # ms
                operation_times.append({
                    "operation": command,
                    "time_ms": op_time,
                    "index": operation_idx
                })
                
                if op_time > SLOW_OPERATION_THRESHOLD:
                    slow_operations.append({
                        "operation": command,
                        "time_ms": op_time,
                        "index": operation_idx,
                        "details": operation
                    })
                    if debug_mode:
                        logger.debug(f"[DEBUG] SLOW OPERATION: {command} took {op_time}ms")
                
            except Exception as e:
                error_msg = f"Error processing operation {operation_idx+1} ({command}): {str(e)}"
                stats["errors"].append(error_msg)
                if debug_mode:
                    logger.error(f"[DEBUG] ERROR: {error_msg}")
                    logger.error(f"[DEBUG] QUERY: {' '.join(operation['tokens'] if isinstance(operation['tokens'], list) else [])}")
                    logger.error(f"[DEBUG] CONTEXT: Operation details:")
                    for key, value in operation.items():
                        if key not in ["tokens", "command"]:
                            logger.error(f"[DEBUG]   - {key}: {value}")
                    import traceback
                    logger.error(f"[DEBUG] TRACEBACK: {traceback.format_exc()}")
        
        # Calculate total time taken
        commit_time_ms = int((time.time() - commit_start_time) * 1000)
        commit_time_sec = commit_time_ms / 1000
        
        # Clear transaction data
        del self.transactions[transaction_id]
        self.active_transaction = None
        
        # Build result message
        result = f"Transaction {transaction_id} committed successfully in "
        
        # Format duration based on how long it took
        if commit_time_sec < 1:
            result += f"{commit_time_ms}ms."
        elif commit_time_sec < 60:
            result += f"{commit_time_sec:.2f} seconds."
        else:
            minutes = int(commit_time_sec / 60)
            seconds = int(commit_time_sec % 60)
            result += f"{minutes}m {seconds}s."
            
        # Add stats summary
        result += f" {stats['operations_processed']} operations processed: "
        if stats["inserts"] > 0:
            result += f"{stats['inserts']} inserts, "
        if stats["updates"] > 0:
            result += f"{stats['updates']} updates, "
        if stats["deletes"] > 0:
            result += f"{stats['deletes']} deletes, "
        if stats["creates"] > 0:
            result += f"{stats['creates']} creates, "
        if stats["drops"] > 0:
            result += f"{stats['drops']} drops, "
        if stats["other"] > 0:
            result += f"{stats['other']} other operations, "
            
        # Remove trailing comma and space
        if result.endswith(", "):
            result = result[:-2]
        
        # Log performance data if it's slow
        if commit_time_ms > 1000:  # Log if total transaction took more than 1 second
            try:
                # Ensure logs directory exists
                os.makedirs("logs", exist_ok=True)
                
                # Log performance data
                with open("logs/transaction_performance.log", "a") as log_file:
                    log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Transaction: {transaction_id}\n")
                    log_file.write(f"Total time: {commit_time_ms}ms, Operations: {stats['operations_processed']}\n")
                    
                    if slow_operations:
                        log_file.write("Slow operations:\n")
                        for op in slow_operations:
                            log_file.write(f"  - {op['operation']} (#{op['index']+1}): {op['time_ms']}ms\n")
                    
                    log_file.write("\n")
            except Exception as e:
                # Don't let logging errors fail the transaction
                if debug_mode:
                    logger.warning(f"[DEBUG] WARNING: Failed to log performance data: {str(e)}")
        
        # Enhanced error reporting
        if stats["errors"]:
            if debug_mode:
                logger.error(f"[DEBUG] Transaction failed with {len(stats['errors'])} errors")
                # Show detailed step analysis
                logger.error(f"[DEBUG] Step analysis:")
                success_count = stats["operations_processed"] - len(stats["errors"])
                logger.error(f"[DEBUG]   Successful steps: {success_count}")
                logger.error(f"[DEBUG]   Failed steps: {len(stats['errors'])}")
                
                # Calculate success rate
                if stats["operations_processed"] > 0:
                    success_rate = (success_count / stats["operations_processed"]) * 100
                    logger.error(f"[DEBUG]   Success rate: {success_rate:.1f}%")
        
        # Add performance insights if available and debug mode is enabled
        if debug_mode and operation_times:
            result += f"\n\nPerformance Summary:"
            result += f"\n - Total commit time: {commit_time_ms}ms"
            result += f"\n - Average operation time: {sum(op['time_ms'] for op in operation_times)/len(operation_times):.1f}ms"
            result += f"\n - Slowest operation: {max(operation_times, key=lambda x: x['time_ms'])['time_ms']}ms (operation #{max(operation_times, key=lambda x: x['time_ms'])['index']+1})"
            
            if slow_operations:
                result += f"\n - Slow operations detected: {len(slow_operations)}"
                for op in slow_operations[:3]:  # Show top 3 slowest for brevity
                    result += f"\n   - #{op['index']+1}: {op['operation']} - {op['time_ms']}ms"
                
                if len(slow_operations) > 3:
                    result += f"\n   - ... {len(slow_operations)-3} more slow operations"
        
        # After successful commit, remove the pending transaction file
        pending_file = os.path.join(self.pending_transactions_path, f"{transaction_id}.json")
        if os.path.exists(pending_file):
            try:
                os.remove(pending_file)
            except Exception as e:
                if debug_mode:
                    logger.warning(f"[DEBUG] Warning: Failed to remove pending transaction file: {str(e)}")
        
        return result
    
    def _process_operation(self, operation, stats, storage, debug_mode=False):
        """Process a single operation during commit"""
        command = operation["command"].upper()
        tokens = operation["tokens"]
        
        if debug_mode:
            logger.debug(f"[DEBUG] Processing {command} operation: {' '.join(tokens) if isinstance(tokens, list) else str(tokens)}")
        
        if command == "INSERT":
            if debug_mode:
                logger.debug(f"[DEBUG] Executing INSERT operation")
            # Execute the actual insert
            from engine.query_processor import process_insert
            result = process_insert(operation, storage)
            stats["inserts"] += 1
            
        elif command == "UPDATE":
            if debug_mode:
                logger.debug(f"[DEBUG] Executing UPDATE operation")
            # Execute the actual update
            from engine.query_processor import process_update
            result = process_update(operation, storage)
            stats["updates"] += 1
            
        elif command == "DELETE":
            if debug_mode:
                logger.debug(f"[DEBUG] Executing DELETE operation")
            # Execute the actual delete
            from engine.query_processor import process_delete
            result = process_delete(operation, storage)
            stats["deletes"] += 1
            
        elif command == "CREATE":
            if debug_mode:
                logger.debug(f"[DEBUG] Executing CREATE operation")
            # Execute the actual create
            from engine.query_processor import process_create
            result = process_create(operation, storage)
            stats["creates"] += 1
            
        elif command == "DROP":
            if debug_mode:
                logger.debug(f"[DEBUG] Executing DROP operation")
            # Execute the actual drop
            from engine.query_processor import process_drop
            result = process_drop(operation, storage)
            stats["drops"] += 1
            
        else:
            if debug_mode:
                logger.debug(f"[DEBUG] Executing other operation type: {command}")
            # Handle other operation types using a dynamic approach
            operation_handler = self._get_operation_handler(command)
            if operation_handler:
                result = operation_handler(operation, storage)
                stats["other"] += 1
            else:
                raise ValueError(f"Unsupported operation type in transaction: {command}")
        
        stats["operations_processed"] += 1
        return result
        
    def rollback_transaction(self, debug_mode=False):
        """Rollback transaction with cleanup of recovery files."""
        if not self.active_transaction:
            return "No active transaction to rollback."
            
        transaction_id = self.active_transaction
        rollback_start_time = time.time()
        
        # Track statistics for reporting
        stats = {
            "tables_created": 0,
            "small_tables_restored": 0,
            "large_tables_restored": 0,
            "very_large_tables_restored": 0,
            "errors": []
        }
        
        if debug_mode:
            logger.debug(f"[DEBUG] Beginning rollback of transaction {transaction_id}")
            if "snapshots" in self.transactions[transaction_id]:
                snapshot_count = len(self.transactions[transaction_id]["snapshots"])
                logger.debug(f"[DEBUG] Found {snapshot_count} tables with snapshots to restore")
            else:
                logger.debug(f"[DEBUG] No snapshots found for transaction")
        
        # Process snapshots in reverse order of operations to handle dependencies
        if "snapshots" in self.transactions[transaction_id]:
            # Track tables that were created during the transaction
            created_tables = set()
            modified_tables = set()
            
            # First pass: identify created vs modified tables
            for table_name, original_data in self.transactions[transaction_id]["snapshots"].items():
                if original_data is None:
                    created_tables.add(table_name)
                else:
                    modified_tables.add(table_name)
            
            if debug_mode:
                logger.debug(f"[DEBUG] Tables created during transaction: {len(created_tables)}")
                logger.debug(f"[DEBUG] Tables modified during transaction: {len(modified_tables)}")
            
            # Import here to avoid circular imports
            from engine.storage import Storage
            storage = Storage()
            
            # Second pass: batch process tables in appropriate order
            
            # 1. Delete tables that were created in the transaction (if they exist)
            if created_tables:
                logger.debug(f"Rolling back {len(created_tables)} tables created in transaction...")
                for table_name in created_tables:
                    if storage.table_exists(table_name):
                        try:
                            if debug_mode:
                                logger.debug(f"[DEBUG] Dropping table '{table_name}' created during transaction")
                            storage._direct_delete_table(table_name)
                            stats["tables_created"] += 1
                        except Exception as e:
                            error_msg = f"Failed to drop table '{table_name}' during rollback: {str(e)}"
                            stats["errors"].append(error_msg)
                            if debug_mode:
                                logger.error(f"[DEBUG] ERROR: {error_msg}")
            
            # 2. Restore tables that were modified but existed before the transaction
            if modified_tables:
                # Group tables by size for more efficient processing
                small_tables = []    # < 1,000 rows
                large_tables = []    # 1,000 - 100,000 rows
                very_large_tables = []  # > 100,000 rows
                
                LARGE_TABLE_THRESHOLD = 1000  # Number of records that constitutes a "large" table
                VERY_LARGE_TABLE_THRESHOLD = 100000  # Number of records that constitutes a "very large" table
                
                for table_name in modified_tables:
                    original_data = self.transactions[transaction_id]["snapshots"][table_name]
                    data_size = len(original_data)
                    
                    if data_size > VERY_LARGE_TABLE_THRESHOLD:
                        very_large_tables.append((table_name, original_data))
                    elif data_size > LARGE_TABLE_THRESHOLD:
                        large_tables.append((table_name, original_data))
                    else:
                        small_tables.append((table_name, original_data))
                
                if debug_mode:
                    logger.debug(f"[DEBUG] Categorized tables by size:")
                    logger.debug(f"[DEBUG] - Small tables (<{LARGE_TABLE_THRESHOLD} rows): {len(small_tables)}")
                    logger.debug(f"[DEBUG] - Large tables ({LARGE_TABLE_THRESHOLD}-{VERY_LARGE_TABLE_THRESHOLD} rows): {len(large_tables)}")
                    logger.debug(f"[DEBUG] - Very large tables (>{VERY_LARGE_TABLE_THRESHOLD} rows): {len(very_large_tables)}")
                
                # Process small tables in batch if possible
                if small_tables:
                    logger.debug(f"Rolling back {len(small_tables)} small tables...")
                    for table_name, original_data in small_tables:
                        try:
                            if debug_mode:
                                logger.debug(f"[DEBUG] Restoring small table '{table_name}' ({len(original_data)} rows)")
                            storage._direct_save_table(table_name, original_data)
                            stats["small_tables_restored"] += 1
                        except Exception as e:
                            error_msg = f"Failed to restore table '{table_name}' during rollback: {str(e)}"
                            stats["errors"].append(error_msg)
                            if debug_mode:
                                logger.error(f"[DEBUG] ERROR: {error_msg}")
                
                # Process large tables individually with progress reports
                if large_tables:
                    logger.debug(f"Rolling back {len(large_tables)} large tables...")
                    for i, (table_name, original_data) in enumerate(large_tables):
                        logger.debug(f"Restoring large table {i+1}/{len(large_tables)}: {table_name} ({len(original_data)} rows)")
                        try:
                            if debug_mode:
                                logger.debug(f"[DEBUG] Starting restore of large table '{table_name}'")
                                start_time = time.time()
                                
                            storage._direct_save_table(table_name, original_data)
                            
                            if debug_mode:
                                elapsed = time.time() - start_time
                                logger.debug(f"[DEBUG] Completed restore of '{table_name}' in {elapsed:.2f}s")
                                
                            stats["large_tables_restored"] += 1
                        except Exception as e:
                            error_msg = f"Failed to restore large table '{table_name}' during rollback: {str(e)}"
                            stats["errors"].append(error_msg)
                            if debug_mode:
                                logger.error(f"[DEBUG] ERROR: {error_msg}")
                
                # Special handling for very large tables - consider chunking or optimizations
                if very_large_tables:
                    logger.debug(f"Rolling back {len(very_large_tables)} very large tables (this may take a while)...")
                    
                    for i, (table_name, original_data) in enumerate(very_large_tables):
                        row_count = len(original_data)
                        logger.debug(f"Restoring very large table {i+1}/{len(very_large_tables)}: {table_name} ({row_count:,} rows)")
                        
                        try:
                            # For extremely large tables, consider alternative approaches like:
                            # 1. Direct file replacement if we have backups
                            # 2. Optimized bulk operations
                            # 3. Chunked processing
                            
                            # For this implementation, we'll still use the direct save but with progress reporting
                            logger.debug(f"Starting restore of {table_name}...")
                            restore_start = time.time()
                            
                            if debug_mode:
                                logger.debug(f"[DEBUG] Starting restore of very large table '{table_name}'")
                            
                            storage._direct_save_table(table_name, original_data)
                            
                            restore_time = time.time() - restore_start
                            logger.debug(f"Restored {table_name} in {restore_time:.2f} seconds ({row_count/restore_time:.0f} rows/sec)")
                            
                            if debug_mode:
                                logger.debug(f"[DEBUG] Restored {row_count} rows in {restore_time:.2f}s ({row_count/restore_time:.0f} rows/sec)")
                            
                            stats["very_large_tables_restored"] += 1
                        except Exception as e:
                            error_msg = f"Failed to restore very large table '{table_name}' during rollback: {str(e)}"
                            stats["errors"].append(error_msg)
                            if debug_mode:
                                logger.error(f"[DEBUG] ERROR: {error_msg}")
                                import traceback
                                logger.error(f"[DEBUG] Traceback: {traceback.format_exc()}")
        
        # Remove the transaction data
        del self.transactions[transaction_id]
        self.active_transaction = None
        
        # Calculate total time taken
        rollback_time_ms = int((time.time() - rollback_start_time) * 1000)
        rollback_time_sec = rollback_time_ms / 1000
        
        if debug_mode:
            logger.debug(f"[DEBUG] Rollback completed in {rollback_time_ms}ms")
        
        # Build result message
        result = f"Transaction {transaction_id} rolled back successfully in "
        
        # Format duration based on how long it took
        if rollback_time_sec < 1:
            result += f"{rollback_time_ms}ms."
        elif rollback_time_sec < 60:
            result += f"{rollback_time_sec:.2f} seconds."
        else:
            minutes = int(rollback_time_sec / 60)
            seconds = int(rollback_time_sec % 60)
            result += f"{minutes}m {seconds}s."
            
        # Add stats summary
        total_tables = (stats["tables_created"] + stats["small_tables_restored"] + 
                        stats["large_tables_restored"] + stats["very_large_tables_restored"])
        
        result += f" {total_tables} tables processed: "
        if stats["tables_created"] > 0:
            result += f"{stats['tables_created']} dropped, "
        if stats["small_tables_restored"] > 0:
            result += f"{stats['small_tables_restored']} small, "
        if stats["large_tables_restored"] > 0:
            result += f"{stats['large_tables_restored']} large, "
        if stats["very_large_tables_restored"] > 0:
            result += f"{stats['very_large_tables_restored']} very large"
            
        # Remove trailing comma and space
        if result.endswith(", "):
            result = result[:-2]
        
        if stats["errors"]:
            result += f" Errors: {len(stats['errors'])}"
            for error in stats["errors"]:
                result += f"\n - {error}"
        
        # After successful rollback, remove the pending transaction file
        pending_file = os.path.join(self.pending_transactions_path, f"{transaction_id}.json")
        if os.path.exists(pending_file):
            try:
                os.remove(pending_file)
            except Exception as e:
                if debug_mode:
                    logger.warning(f"[DEBUG] Warning: Failed to remove pending transaction file: {str(e)}")
        
        return result

    def replay_transactions(self):
        """Replay transactions from the log file for crash recovery."""
        if not os.path.exists(self.transaction_log):
            return "No transaction log found."

        with open(self.transaction_log, "r") as log_file:
            for line in log_file:
                transaction = json.loads(line.strip())
                transaction_id, transaction_data = next(iter(transaction.items()))
                # Here you would apply each transaction to the database
                # For now, we just print the transaction
                logger.info(f"Replaying transaction {transaction_id}: {transaction_data}")

        return "Transactions replayed."

    def create_savepoint(self, savepoint_name=None):
        """Create a savepoint in the current transaction."""
        if not self.active_transaction:
            return "No active transaction. Begin a transaction first."
        
        # Generate a savepoint name if not provided
        if not savepoint_name:
            savepoint_name = f"SP-{int(time.time())}-{random.randint(1000, 9999)}"
        
        # Initialize savepoints tracking if needed
        if "savepoints" not in self.transactions[self.active_transaction]:
            self.transactions[self.active_transaction]["savepoints"] = {}
        
        # Store the current state of the transaction at this savepoint
        savepoint_data = {
            "operations_index": len(self.transactions[self.active_transaction]["operations"]),
            "snapshots": {}, # We'll copy only relevant snapshots
            "time": time.time()
        }
        
        # Only copy snapshots for tables modified after the previous savepoint
        prev_snapshots = self.transactions[self.active_transaction]["snapshots"]
        for table_name, snapshot in prev_snapshots.items():
            # Copy the snapshot reference, not the data (to save memory)
            savepoint_data["snapshots"][table_name] = snapshot
        
        # Store the savepoint
        self.transactions[self.active_transaction]["savepoints"][savepoint_name] = savepoint_data
        
        return f"Savepoint '{savepoint_name}' created."

    def rollback_to_savepoint(self, savepoint_name, debug_mode=False):
        """Rollback to a specific savepoint in the current transaction."""
        if not self.active_transaction:
            return "No active transaction to rollback."
        
        tx_data = self.transactions[self.active_transaction]
        
        # Check if savepoints are tracked and the specified savepoint exists
        if "savepoints" not in tx_data or savepoint_name not in tx_data["savepoints"]:
            return f"Savepoint '{savepoint_name}' not found in current transaction."
        
        savepoint = tx_data["savepoints"][savepoint_name]
        operations_index = savepoint["operations_index"]
        
        if debug_mode:
            logger.debug(f"[DEBUG] Rolling back to savepoint '{savepoint_name}'")
            logger.debug(f"[DEBUG] Operations to keep: {operations_index} out of {len(tx_data['operations'])}")
        
        # Rollback only the operations after the savepoint
        operations_to_rollback = tx_data["operations"][operations_index:]
        
        # Keep track of affected tables for partial rollback
        affected_tables = set()
        for op in operations_to_rollback:
            if "table_name" in op:
                affected_tables.add(op["table_name"])
        
        if debug_mode:
            logger.debug(f"[DEBUG] Tables affected by partial rollback: {', '.join(affected_tables)}")
        
        # Import storage for rollback operations
        from engine.storage import Storage
        storage = Storage()
        
        # Roll back changes to the affected tables
        roll_stats = {
            "tables_reverted": 0,
            "errors": []
        }
        
        for table_name in affected_tables:
            try:
                if table_name in savepoint["snapshots"]:
                    original_data = savepoint["snapshots"][table_name]
                    
                    if original_data is None:
                        # Table was created after this savepoint, drop it
                        if storage.table_exists(table_name):
                            if debug_mode:
                                logger.debug(f"[DEBUG] Dropping table '{table_name}' created after savepoint")
                            storage._direct_delete_table(table_name)
                            roll_stats["tables_reverted"] += 1
                    else:
                        # Table was modified after savepoint, restore it
                        if debug_mode:
                            logger.debug(f"[DEBUG] Restoring table '{table_name}' to savepoint state")
                        storage._direct_save_table(table_name, original_data)
                        roll_stats["tables_reverted"] += 1
            except Exception as e:
                error_msg = f"Error restoring table '{table_name}' during partial rollback: {str(e)}"
                roll_stats["errors"].append(error_msg)
                if debug_mode:
                    logger.error(f"[DEBUG] ERROR: {error_msg}")
        
        # Truncate the operations list to the savepoint
        tx_data["operations"] = tx_data["operations"][:operations_index]
        
        # Remove savepoints created after this one
        savepoints_to_remove = []
        for sp_name, sp_data in tx_data["savepoints"].items():
            if sp_data["time"] > savepoint["time"]:
                savepoints_to_remove.append(sp_name)
        
        for sp_name in savepoints_to_remove:
            del tx_data["savepoints"][sp_name]
        
        result = f"Rolled back to savepoint '{savepoint_name}'. {roll_stats['tables_reverted']} tables restored."
        
        if roll_stats["errors"]:
            result += f" Errors: {len(roll_stats['errors'])}"
            for error in roll_stats["errors"]:
                result += f"\n - {error}"
            
        return result

    def auto_recover_transactions(self):
        """Automatically recover pending transactions after a crash."""
        if not os.path.exists(self.pending_transactions_path):
            return
        
        pending_files = [f for f in os.listdir(self.pending_transactions_path) if f.endswith('.json')]
        
        if not pending_files:
            return
        
        logger.info(f"Found {len(pending_files)} pending transactions to recover")
        
        recovered = 0
        failed = 0
        
        for pending_file in pending_files:
            file_path = os.path.join(self.pending_transactions_path, pending_file)
            try:
                with open(file_path, "r") as f:
                    tx_data = json.load(f)
                    
                transaction_id = tx_data.get("id")
                operations = tx_data.get("operations", [])
                
                if not transaction_id or not operations:
                    logger.warning(f"Invalid pending transaction data in {pending_file}")
                    os.remove(file_path)
                    failed += 1
                    continue
                    
                logger.info(f"Recovering transaction {transaction_id} with {len(operations)} operations")
                
                # Restore transaction in memory
                self.transactions[transaction_id] = {
                    "operations": operations,
                    "snapshots": {},  # We'll need to rebuild snapshots
                    "start_time": tx_data.get("start_time", time.time()),
                    "affected_tables": set(),
                    "recovery": True
                }
                
                # Extract affected tables from operations
                for op in operations:
                    if "table_name" in op:
                        self.transactions[transaction_id]["affected_tables"].add(op["table_name"])
                
                # Capture snapshots for all affected tables for potential rollback
                for table_name in self.transactions[transaction_id]["affected_tables"]:
                    self._capture_table_snapshot(table_name)
                    
                # Set as active transaction and commit it
                self.active_transaction = transaction_id
                result = self.commit_transaction(debug_mode=True)
                
                logger.info(f"Auto-recovered transaction {transaction_id}: {result}")
                recovered += 1
                
            except Exception as e:
                logger.error(f"Failed to recover transaction from {pending_file}: {str(e)}")
                # Mark as failed but don't delete the file for manual investigation
                failed += 1
        
        logger.info(f"Transaction recovery complete. Recovered: {recovered}, Failed: {failed}")
        return f"Recovered {recovered} transactions, {failed} failed"

    def _get_operation_handler(self, command):
        """Get the handler function for a specific operation."""
        # Convert SQL command to operation type
        operation_type = command.lower()
        
        # Check if we have a dedicated handler
        if operation_type in self.operation_handlers:
            return self.operation_handlers[operation_type]
        
        return None
        
    def _apply_save_operation(self, operation, storage):
        """Apply a save operation to the database."""
        if "table_name" not in operation or "data" not in operation:
            logger.error("Invalid save operation: missing table_name or data")
            return False
        
        table_name = operation["table_name"]
        data = operation["data"]
        logger.debug(f"Applying save operation for table {table_name}")
        return storage._direct_save_table(table_name, data)
            
    def _apply_delete_operation(self, operation, storage):
        """Apply a delete operation to the database."""
        if "table_name" not in operation:
            logger.error("Invalid delete operation: missing table_name")
            return False
        
        table_name = operation["table_name"]
        logger.debug(f"Applying delete operation for table {table_name}")
        return storage._direct_delete_table(table_name)

    def get_active_transactions(self):
        """Get a list of all active transactions with details.
        
        Returns:
            list: A list of dictionary objects containing transaction details
        """
        transaction_list = []
        
        # Get current time for calculating duration
        current_time = time.time()
        
        # First, add the current active transaction if there is one
        if self.active_transaction:
            tx_data = self.transactions[self.active_transaction]
            
            # Calculate transaction duration in seconds
            duration = current_time - tx_data["start_time"]
            
            # Format the transaction details
            transaction_list.append({
                "id": self.active_transaction,
                "start_time": datetime.fromtimestamp(tx_data["start_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                "duration": f"{int(duration // 60)}m {int(duration % 60)}s",
                "duration_seconds": duration,
                "operations": len(tx_data["operations"]),
                "affected_tables": list(tx_data.get("affected_tables", set())),
                "status": "active",
                "is_current": True
            })
        
        # Then add any other transactions that are in the transactions dict
        # but are not the active one
        for tx_id, tx_data in self.transactions.items():
            if tx_id != self.active_transaction:
                # Calculate transaction duration
                duration = current_time - tx_data["start_time"]
                
                # Format the transaction details
                transaction_list.append({
                    "id": tx_id,
                    "start_time": datetime.fromtimestamp(tx_data["start_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "duration": f"{int(duration // 60)}m {int(duration % 60)}s",
                    "duration_seconds": duration,
                    "operations": len(tx_data["operations"]),
                    "affected_tables": list(tx_data.get("affected_tables", set())),
                    "status": "pending",
                    "is_current": False
                })
        
        # Sort transactions by start time (newest first)
        transaction_list.sort(key=lambda x: x["duration_seconds"], reverse=True)
        
        return transaction_list
        
    def show_transactions(self):
        """Format and return information about all active transactions.
        
        Returns:
            str: Formatted string with transaction details
        """
        transactions = self.get_active_transactions()
        
        if not transactions:
            return "No active transactions."
            
        # Format the output
        output = ["ACTIVE TRANSACTIONS:", ""]
        output.append(f"{'ID':<15} {'START TIME':<20} {'DURATION':<12} {'OPERATIONS':<10} {'AFFECTED TABLES':<30}")
        output.append("-" * 90)
        
        for tx in transactions:
            # Mark the current transaction with an asterisk
            current_marker = "*" if tx.get("is_current", False) else " "
            
            # Format the list of affected tables
            affected_tables = ", ".join(tx["affected_tables"]) if tx["affected_tables"] else "None"
            if len(affected_tables) > 30:
                affected_tables = affected_tables[:27] + "..."
                
            # Add the transaction row
            output.append(f"{current_marker} {tx['id']:<14} {tx['start_time']:<20} {tx['duration']:<12} {tx['operations']:<10} {affected_tables:<30}")
            
        return "\n".join(output)
