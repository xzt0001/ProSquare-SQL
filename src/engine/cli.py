from engine.sql_parser import SQLParser
from engine.query_executor import QueryExecutor
import time
import os
import sys

class SQLCLI:
    def __init__(self):
        self.parser = SQLParser()
        self.executor = QueryExecutor()
        self.transaction_active = False
        self.transaction_start_time = None
        self.transaction_warning_threshold = 300  # 5 minutes without commit
        self.supports_color = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
        self.exit_requested = False
        self.debug_mode = False  # Flag to control transaction debugging
        self.history = []

    def _colorize(self, text, color_code):
        """Add color to text if terminal supports it."""
        if self.supports_color:
            return f"\033[{color_code}m{text}\033[0m"
        return text

    def start(self):
        """Starts an interactive SQL REPL with transaction support."""
        print("Welcome to ProSquare SQL! Type 'exit' to quit.")
        print("Transaction commands supported: BEGIN TRANSACTION, COMMIT, ROLLBACK")
        print("Type 'help' for more information or 'status' to check transaction status.")
        
        while not self.exit_requested:
            # Check for long-running transactions
            self._check_transaction_state()
            
            # Show transaction status in prompt
            if self.transaction_active:
                # Get transaction ID
                tx_id = "unknown"
                tx_ops = 0
                if self.executor.transaction_manager.active_transaction:
                    tx_id = self.executor.transaction_manager.active_transaction[:8]
                    if self.executor.transaction_manager.active_transaction in self.executor.transaction_manager.transactions:
                        tx_ops = len(self.executor.transaction_manager.transactions[self.executor.transaction_manager.active_transaction]["operations"])
                
                # Format transaction prompt with color if supported
                tx_prompt = self._colorize(f"SQL (TX:{tx_id} | Ops:{tx_ops})> ", "1;33")  # Bold yellow
            else:
                tx_prompt = "SQL> "
                
            # Debug indicator
            debug_indicator = ""
            if self.debug_mode:
                if self.supports_color:
                    debug_indicator = f"{self._colorize('[DEBUG]', '1;36')} "
                
            query = input(f"{debug_indicator}{tx_prompt}")
            
            if query.lower() == "exit":
                # Check if a transaction is active before exiting
                if self.transaction_active:
                    print(self._colorize("\n⚠️  WARNING: UNCOMMITTED TRANSACTION ⚠️", "1;31;47"))  # Bold red with white background
                    print(self._colorize("You have an active transaction with pending changes.", "1;31"))
                    print("Options:")
                    print("  1. Type 'commit' to save all changes")
                    print("  2. Type 'rollback' to discard all changes")
                    print("  3. Type 'exit confirm' to exit WITHOUT saving (ALL CHANGES WILL BE LOST)")
                    return
                elif query.lower() == "exit confirm" and self.exit_warned:
                    # Second confirmation received
                    self.exit_requested = True
                    return
                else:
                    # No transaction active, normal exit
                    self.exit_requested = True
                    return
            elif query.lower() in ["help", "\\h", "\\?"]:
                self._display_help()
                continue
            elif query.lower() in ["status", "\\s"]:
                self._display_transaction_status()
                continue
                
            # Handle special transaction commands that might not parse correctly
            if query.lower() in ["begin transaction", "begin", "start transaction"]:
                try:
                    result = self.executor.execute({"command": "BEGIN", "tokens": ["BEGIN", "TRANSACTION"]})
                    if "error" in result:
                        print(self._colorize(f"❌ Error: {result['error']}", "1;31"))
                    else:
                        self.transaction_active = True
                        self.transaction_start_time = time.time()
                        print(self._colorize("🔄 " + result.get("message", "Transaction started."), "1;32"))
                except Exception as e:
                    print(self._colorize(f"❌ Error starting transaction: {e}", "1;31"))
                continue
            elif query.lower() == "commit":
                try:
                    result = self.executor.execute({"command": "COMMIT", "tokens": ["COMMIT"]})
                    if "error" in result:
                        print(self._colorize(f"❌ Error: {result['error']}", "1;31"))
                    else:
                        self.transaction_active = False
                        self.transaction_start_time = None
                        print(self._colorize("✅ " + result.get("message", "Transaction committed."), "1;32"))
                except Exception as e:
                    print(self._colorize(f"❌ Error committing transaction: {e}", "1;31"))
                continue
            elif query.lower() == "rollback":
                try:
                    result = self.executor.execute({"command": "ROLLBACK", "tokens": ["ROLLBACK"]})
                    if "error" in result:
                        print(self._colorize(f"❌ Error: {result['error']}", "1;31"))
                    else:
                        self.transaction_active = False
                        self.transaction_start_time = None
                        print(self._colorize("⏮️ " + result.get("message", "Transaction rolled back."), "1;33"))
                except Exception as e:
                    print(self._colorize(f"❌ Error rolling back transaction: {e}", "1;31"))
                continue
                
            # Handle standard SQL queries
            try:
                parsed_query = self.parser.parse(query)
                command = parsed_query["command"].upper()
                
                # Show transaction status if inside transaction
                if self.transaction_active and command in ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP"]:
                    print(self._colorize(f"📝 Operation '{command}' will be added to current transaction", "1;36"))
                
                # Pass all queries to execute for standard processing
                start_time = time.time()
                result = self.executor.execute(parsed_query, debug=self.debug_mode)
                end_time = time.time()
                execution_time_ms = round((end_time - start_time) * 1000, 2)
                
                # Set transaction status based on executor operation
                if command == "BEGIN":
                    self.transaction_active = True
                    self.transaction_start_time = time.time()
                elif command in ["COMMIT", "ROLLBACK"]:
                    self.transaction_active = False
                    self.transaction_start_time = None
                    
                # Update transaction status based on the executor's transaction state
                self.transaction_active = self.executor.transaction_manager.active_transaction is not None
                
                # Pretty print the result
                if isinstance(result, dict):
                    if "error" in result:
                        print(self._colorize(f"❌ Error: {result['error']}", "1;31"))
                    elif "message" in result:
                        # Use appropriate styling based on message type
                        if "committed" in result["message"].lower():
                            print(self._colorize("✅ " + result["message"], "1;32"))
                        elif "rolled back" in result["message"].lower():
                            print(self._colorize("⏮️ " + result["message"], "1;33"))
                        elif "transaction" in result["message"].lower():
                            print(self._colorize("🔄 " + result["message"], "1;36"))
                        else:
                            print(result["message"])
                    else:
                        print("Result:", result)
                else:
                    print("Result:", result)
                    
                # Show affected rows for data modification operations
                if command in ["INSERT", "UPDATE", "DELETE"] and isinstance(result, dict) and "affected_rows" in result:
                    affected = result["affected_rows"]
                    if affected > 0:
                        print(self._colorize(f"📊 Affected rows: {affected}", "1;32"))
                    else:
                        print(self._colorize(f"ℹ️ Affected rows: {affected}", "1;36"))
                    
                # If in debug mode, show execution time
                if self.debug_mode:
                    print(self._colorize(f"⏱ Query executed in {execution_time_ms}ms", "1;36"))
                    
                    # If result has performance data, show that too
                    if isinstance(result, dict) and "performance" in result:
                        perf_data = result["performance"]
                        print(self._colorize(f"🔍 Performance details:", "1;36"))
                        for key, value in perf_data.items():
                            print(f"  - {key}: {value}")
                
            except Exception as e:
                print(self._colorize(f"❌ Error: {e}", "1;31"))
                
    def _check_transaction_state(self):
        """Check if transaction has been active for too long and warn user."""
        if not self.transaction_active or not self.transaction_start_time:
            return
            
        elapsed_time = time.time() - self.transaction_start_time
        if elapsed_time > self.transaction_warning_threshold:
            # Calculate duration in minutes
            minutes = int(elapsed_time / 60)
            print(self._colorize(f"\n⚠️  WARNING: Transaction has been active for {minutes} minutes without commit.", "1;31"))
            print(self._colorize("Consider committing or rolling back to avoid losing changes.", "1;31"))
            
    def _display_transaction_status(self):
        """Display detailed information about the current transaction state."""
        if not self.transaction_active:
            print("No active transaction.")
            return
            
        # Get transaction details
        tx_id = "unknown"
        tx_ops = 0
        tx_tables = set()
        tx_start = "unknown"
        
        if self.executor.transaction_manager.active_transaction:
            tx_id = self.executor.transaction_manager.active_transaction
            
            # Get transaction data
            tx_data = self.executor.transaction_manager.transactions.get(tx_id, {})
            operations = tx_data.get("operations", [])
            tx_ops = len(operations)
            tx_start = tx_data.get("start_time", "unknown")
            
            # Extract affected tables
            for op in operations:
                if isinstance(op, dict) and "table_name" in op:
                    tx_tables.add(op["table_name"])
        
        # Calculate duration
        duration = ""
        if self.transaction_start_time:
            elapsed = time.time() - self.transaction_start_time
            if elapsed < 60:
                duration = f"{int(elapsed)} seconds"
            else:
                minutes = int(elapsed / 60)
                seconds = int(elapsed % 60)
                duration = f"{minutes} minutes, {seconds} seconds"
                
        # Display transaction information
        print(self._colorize("=== TRANSACTION STATUS ===", "1;36"))
        print(f"Transaction ID: {self._colorize(tx_id, '1;33')}")
        print(f"Started: {tx_start}")
        print(f"Duration: {duration}")
        print(f"Operations pending: {self._colorize(str(tx_ops), '1;33')}")
        print(f"Tables affected: {', '.join(sorted(tx_tables)) if tx_tables else 'None'}")
        print(self._colorize("=========================", "1;36"))
                
    def _display_help(self):
        """Display help information for the SQL CLI."""
        print("\nProSquare SQL Help:")
        print(self._colorize("-------------------", "1;36"))
        print(self._colorize("Transaction Commands:", "1;33"))
        print("  BEGIN TRANSACTION - Start a new transaction")
        print("  COMMIT - Commit all changes in the current transaction")
        print("  ROLLBACK - Undo all changes in the current transaction")
        print(self._colorize("\nOther Commands:", "1;33"))
        print("  exit - Exit the SQL shell")
        print("  help - Display this help information")
        print("  status - Show current transaction details")
        print(self._colorize("\nTransaction Status:", "1;33"))
        print("  The prompt will show transaction ID and pending operation count when active")
        print("  You will be warned if a transaction is active for more than 5 minutes")
        print(self._colorize("-------------------", "1;36"))

    def _process_command(self, command):
        """Process a command entered in the CLI"""
        command = command.strip()
        if not command:
            return
        
        # Keep history of commands
        self.history.append(command)
        
        # Handle exit commands
        if command in ('exit', '\\q'):
            if self.transaction_active:
                print("\n⚠️  WARNING: You have an active transaction with uncommitted changes!")
                print("    Type 'commit' to save changes or 'rollback' to discard them before exiting.")
                print("    Or type 'exit' again to exit without committing (data will be lost).\n")
                if not self.exit_requested:
                    self.exit_requested = True
                    return
            self.exit_requested = True
            return
        elif self.exit_requested:
            # Reset the exit flag if they typed something else after the first exit
            self.exit_requested = False
        
        # Help command
        if command in ('help', '\\h'):
            self._show_help()
            return
        
        # Status command
        if command in ('status', '\\s'):
            self._show_status()
            return
            
        # Debug mode toggle
        if command in ('debug', '\\d'):
            self.debug_mode = not self.debug_mode
            if self.debug_mode:
                print("Transaction debug mode enabled. Verbose logging will be shown for all transaction operations.")
            else:
                print("Transaction debug mode disabled.")
            return
        
        # Transaction commands
        if command == 'begin':
            self._handle_begin_transaction()
            return
        
        if command == 'commit':
            self._handle_commit_transaction()
            return
            
        if command == 'rollback':
            self._handle_rollback_transaction()
            return
        
        # Execute SQL commands
        # Pass debug mode flag to engine for verbose logging
        result = self.executor.execute(command, debug=self.debug_mode)
        print(result)
    
    def _show_help(self):
        """Show help information"""
        help_msg = """
Available commands:
  help, \\h           Show this help message
  exit, \\q           Exit the CLI
  status, \\s         Show transaction status
  debug, \\d          Toggle transaction debug mode
  
Transaction Commands:
  begin               Start a new transaction
  commit              Commit the current transaction
  rollback            Rollback the current transaction

SQL Commands:
  CREATE TABLE ...    Create a new table
  INSERT INTO ...     Insert data into a table
  SELECT ...          Query data from a table
  UPDATE ...          Update data in a table
  DELETE FROM ...     Delete data from a table
  DROP TABLE ...      Drop a table
        """
        print(help_msg)
    
    def _handle_begin_transaction(self):
        """Handle 'begin' command to start a transaction"""
        if self.transaction_active:
            print("Transaction already active. Commit or rollback first.")
            return
            
        result = self.executor.execute({"command": "BEGIN", "tokens": ["BEGIN", "TRANSACTION"]})
        print(result)
        self.transaction_active = True
        self.transaction_start_time = time.time()
    
    def _handle_commit_transaction(self):
        """Handle 'commit' command to commit a transaction"""
        if not self.transaction_active:
            print("No active transaction to commit.")
            return
            
        result = self.executor.execute({"command": "COMMIT", "tokens": ["COMMIT"]})
        print(result)
        self.transaction_active = False
        self.transaction_start_time = None
    
    def _handle_rollback_transaction(self):
        """Handle 'rollback' command to rollback a transaction"""
        if not self.transaction_active:
            print("No active transaction to rollback.")
            return
            
        result = self.executor.execute({"command": "ROLLBACK", "tokens": ["ROLLBACK"]})
        print(result)
        self.transaction_active = False
        self.transaction_start_time = None

# Example Usage:
# cli = SQLCLI()
# cli.start()

