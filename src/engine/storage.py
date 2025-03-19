import os
import json
import shutil
import time
import threading
import logging
import hashlib
from pathlib import Path
from collections import defaultdict
from engine.transaction import TransactionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("storage.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('ProSquareSQL')

class StorageError(Exception):
    """Custom exception for storage-related errors with detailed context."""
    def __init__(self, message, table_name=None, error_type="STORAGE_ERROR"):
        self.message = message
        self.table_name = table_name
        self.error_type = error_type
        super().__init__(self.message)
        
    def __str__(self):
        error_msg = f"{self.error_type}: {self.message}"
        if self.table_name:
            error_msg += f" (Table: {self.table_name})"
        return error_msg

class Storage:
    DATA_DIR = "data"
    BACKUP_DIR = "data/backup"
    SCHEMA_DIR = "data/schemas"
    
    def __init__(self):
        """Ensures the data storage directory exists."""
        # Create data directory if it doesn't exist
        Path(self.DATA_DIR).mkdir(exist_ok=True)
        
        # Create backup directory if it doesn't exist
        Path(self.BACKUP_DIR).mkdir(exist_ok=True, parents=True)
        
        # Create schema directory if it doesn't exist
        Path(self.SCHEMA_DIR).mkdir(exist_ok=True, parents=True)
        
        # Lock for thread safety during file operations
        self.file_locks = {}
        self.lock_mutex = threading.Lock()
        
        # Keep track of recoveries to avoid infinite loops
        self.recovery_attempts = {}
        
        # In-memory schema cache
        self.schema_cache = {}
        
        # Data integrity options
        self.checksum_validation = True
        self.strict_schema_validation = False  # Set to True for strict mode
        
        # Performance monitoring
        self.slow_operation_threshold_ms = 200  # Log warnings for operations slower than 200ms
        self.very_slow_operation_threshold_ms = 1000  # Log errors for operations slower than 1000ms
        
        # Log storage initialization
        logger.info("Storage system initialized with data directory: " + os.path.abspath(self.DATA_DIR))

        self.transaction_manager = TransactionManager()

    def _log_performance(self, operation, table_name, start_time):
        """Log performance metrics for operations."""
        end_time = time.time()
        elapsed_ms = int((end_time - start_time) * 1000)
        
        # Always log performance metrics at debug level
        logger.debug(f"{operation} on table '{table_name}' took {elapsed_ms}ms")
        
        # Log warnings for slow operations
        if elapsed_ms > self.very_slow_operation_threshold_ms:
            logger.error(f"VERY SLOW OPERATION: {operation} on table '{table_name}' took {elapsed_ms}ms")
        elif elapsed_ms > self.slow_operation_threshold_ms:
            logger.warning(f"SLOW OPERATION: {operation} on table '{table_name}' took {elapsed_ms}ms")
            
        return elapsed_ms

    def save_table(self, table_name, data):
        """Saves table data with transaction awareness."""
        start_time = time.time()
        
        # Validate table name regardless of transaction status
        try:
            self._validate_table_name(table_name)
        except StorageError as e:
            # If in a transaction, fail early
            if self.transaction_manager.active_transaction:
                raise e
        
        # Validate data is a list
        if not isinstance(data, list):
            raise StorageError("Table data must be a list of records", table_name, "TYPE_ERROR")
        
        if self.transaction_manager.active_transaction:
            # Queue the operation if a transaction is active
            self.transaction_manager.log_operation({
                "operation": "save",
                "table_name": table_name,
                "data": data
            })
            self._log_performance("Queue save", table_name, start_time)
            return True

        # Direct save for non-transactional operations
        result = self._direct_save_table(table_name, data)
        self._log_performance("Save", table_name, start_time)
        return result
        
    def _direct_save_table(self, table_name, data):
        """Internal method to save table data directly (used by transaction commit)."""
        start_time = time.time()
        
        self._validate_table_name(table_name)
        
        # Validate data is a list
        if not isinstance(data, list):
            raise StorageError("Table data must be a list of records", table_name, "TYPE_ERROR")
            
        table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
        
        # Get lock for this table
        lock = self._get_file_lock(table_name)
        
        with lock:
            # Perform data validation before saving
            try:
                self._validate_data_integrity(table_name, data)
            except StorageError as e:
                logger.error(f"Data integrity validation failed for {table_name}: {str(e)}")
                self._log_performance("Failed save (validation)", table_name, start_time)
                raise
            
            # Write the new data
            temp_path = f"{table_path}.tmp"
            try:
                # Serialize to JSON with checksums
                data_with_checksum = self._add_checksum_to_data(data)
                
                with open(temp_path, "w", encoding="utf-8") as file:
                    json.dump(data_with_checksum, file, indent=4)
                    file.flush()
                    os.fsync(file.fileno())  # Ensure data is written to disk
                
                # Atomically replace the old file with the new one
                os.replace(temp_path, table_path)
                
                # Update or create schema
                self._update_schema(table_name, data)
                
                # Verify the file to ensure it was written correctly
                try:
                    self._verify_table_integrity(table_path)
                except Exception as e:
                    logger.error(f"Integrity check failed after saving {table_name}: {str(e)}")
                    # Auto-recover from the most recent backup if verification fails
                    if self._auto_recover_from_backup(table_name):
                        logger.info(f"Auto-recovered table {table_name} after failed integrity check")
                    else:
                        logger.error(f"Failed to auto-recover table {table_name}")
                        self._log_performance("Failed save (integrity)", table_name, start_time)
                        raise StorageError(f"Failed to save table with integrity: {str(e)}", table_name, "INTEGRITY_ERROR")
                
                elapsed_ms = self._log_performance("Direct save", table_name, start_time)
                # Log additional info for large tables
                if len(data) > 1000:
                    logger.info(f"Saved large table '{table_name}' with {len(data)} records in {elapsed_ms}ms")
                
                return True
            except Exception as e:
                # Clean up temp file if it exists
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                self._log_performance("Failed save", table_name, start_time)
                raise StorageError(f"Failed to save table: {str(e)}", table_name, "WRITE_ERROR")

    def _add_checksum_to_data(self, data):
        """Add a checksum to the data for integrity verification."""
        # Create a copy of the data to avoid modifying the original
        data_copy = {"rows": data}
        
        # Calculate checksum
        data_str = json.dumps(data, sort_keys=True)
        checksum = hashlib.md5(data_str.encode()).hexdigest()
        
        # Add checksum and metadata
        data_copy["__metadata"] = {
            "checksum": checksum,
            "row_count": len(data),
            "timestamp": time.time()
        }
        
        return data_copy
        
    def _extract_data_from_checksum(self, data_with_checksum):
        """Extract data from a checksum-protected object and verify integrity."""
        if not isinstance(data_with_checksum, dict):
            return data_with_checksum  # Old format without checksum
            
        # Extract the actual data
        if "rows" not in data_with_checksum:
            return data_with_checksum  # Old format or invalid format
            
        data = data_with_checksum["rows"]
        
        # Verify checksum if enabled
        if self.checksum_validation and "__metadata" in data_with_checksum:
            metadata = data_with_checksum["__metadata"]
            if "checksum" in metadata:
                expected_checksum = metadata["checksum"]
                data_str = json.dumps(data, sort_keys=True)
                actual_checksum = hashlib.md5(data_str.encode()).hexdigest()
                
                if expected_checksum != actual_checksum:
                    raise StorageError("Data integrity check failed: checksum mismatch", None, "CHECKSUM_ERROR")
                    
        return data
    
    def _validate_data_integrity(self, table_name, data):
        """Validate data integrity before saving."""
        # Basic validation - check that data is a list
        if not isinstance(data, list):
            raise StorageError("Data must be a list of records", table_name, "TYPE_ERROR")
            
        # Empty data is valid
        if not data:
            return
            
        # Validate each record is a dictionary
        for i, record in enumerate(data):
            if not isinstance(record, dict):
                raise StorageError(f"Record at index {i} is not a dictionary", table_name, "TYPE_ERROR")
                
        # Load schema for validation if it exists and strict validation is enabled
        if self.strict_schema_validation:
            schema = self._load_schema(table_name)
            if schema:
                self._validate_against_schema(table_name, data, schema)
    
    def _validate_against_schema(self, table_name, data, schema):
        """Validate data against a schema."""
        expected_fields = schema.get("fields", {})
        nullable_fields = schema.get("nullable_fields", [])
        
        for i, record in enumerate(data):
            # Check for required fields
            for field, field_type in expected_fields.items():
                if field not in record:
                    if field not in nullable_fields:
                        raise StorageError(f"Required field '{field}' missing in record at index {i}", 
                                        table_name, "SCHEMA_ERROR")
                elif record[field] is not None:
                    # Type validation for non-null fields
                    valid = False
                    if field_type == "string" and isinstance(record[field], str):
                        valid = True
                    elif field_type == "number" and isinstance(record[field], (int, float)):
                        valid = True
                    elif field_type == "boolean" and isinstance(record[field], bool):
                        valid = True
                    elif field_type == "object" and isinstance(record[field], dict):
                        valid = True
                    elif field_type == "array" and isinstance(record[field], list):
                        valid = True
                        
                    if not valid:
                        raise StorageError(f"Field '{field}' in record at index {i} has invalid type. "
                                        f"Expected {field_type}, got {type(record[field]).__name__}", 
                                        table_name, "TYPE_ERROR")
    
    def _verify_table_integrity(self, table_path):
        """Verifies that a table file contains valid JSON and expected format."""
        with open(table_path, "r", encoding="utf-8") as file:
            try:
                data_with_checksum = json.load(file)
                
                # Extract data from checksum-protected object
                if isinstance(data_with_checksum, dict) and "rows" in data_with_checksum:
                    data = data_with_checksum["rows"]
                    
                    # Verify checksum if available
                    if self.checksum_validation and "__metadata" in data_with_checksum:
                        metadata = data_with_checksum["__metadata"]
                        if "checksum" in metadata:
                            expected_checksum = metadata["checksum"]
                            expected_row_count = metadata.get("row_count", 0)
                            
                            # Verify row count
                            if len(data) != expected_row_count:
                                raise StorageError(f"Row count mismatch: expected {expected_row_count}, got {len(data)}", 
                                                None, "INTEGRITY_ERROR")
                            
                            # Verify checksum
                            data_str = json.dumps(data, sort_keys=True)
                            actual_checksum = hashlib.md5(data_str.encode()).hexdigest()
                            
                            if expected_checksum != actual_checksum:
                                raise StorageError("Checksum mismatch", None, "CHECKSUM_ERROR")
                else:
                    # Legacy format without checksums
                    data = data_with_checksum
                
                # Check that data is a list
                if not isinstance(data, list):
                    raise StorageError("Table data is not a list", None, "TYPE_ERROR")
                    
                # Check each record is a dictionary
                for i, record in enumerate(data):
                    if not isinstance(record, dict):
                        raise StorageError(f"Record at index {i} is not a dictionary", None, "TYPE_ERROR")
                        
            except json.JSONDecodeError:
                raise StorageError("Invalid JSON format", None, "JSON_ERROR")
    
    def backup_table(self, table_name):
        """Creates a backup of a table before making changes."""
        self._validate_table_name(table_name)
        table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
        backup_path = os.path.join(self.BACKUP_DIR, f"{table_name}_{int(time.time())}.json")
        
        # Get lock for this table
        lock = self._get_file_lock(table_name)
        
        with lock:
            # Create backup of existing file if it exists
            if os.path.exists(table_path):
                try:
                    shutil.copy2(table_path, backup_path)
                    
                    # Verify backup integrity
                    try:
                        self._verify_table_integrity(backup_path)
                        logger.info(f"Created backup of {table_name}")
                        return True
                    except Exception as e:
                        logger.error(f"Backup integrity check failed for {table_name}: {str(e)}")
                        # If the backup is corrupted, try to use the previous backup
                        os.remove(backup_path)
                        return False
                        
                except Exception as e:
                    logger.error(f"Failed to create backup of {table_name}: {str(e)}")
                    return False
            return False

    def get_table(self, table_name):
        """Retrieves table data from storage with error handling and recovery."""
        self._validate_table_name(table_name)
        table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
        
        # Get lock for this table
        lock = self._get_file_lock(table_name)
        
        with lock:
            if not os.path.exists(table_path):
                return []
            
            # Reset recovery attempts for this table
            if table_name in self.recovery_attempts:
                self.recovery_attempts[table_name] = 0
                
            try:
                with open(table_path, "r", encoding="utf-8") as file:
                    data_with_checksum = json.load(file)
                    
                    # Extract data and verify checksum
                    data = self._extract_data_from_checksum(data_with_checksum)
                    
                    # Validate format
                    if not isinstance(data, list):
                        raise StorageError(f"Invalid table format: expected a list", table_name, "TYPE_ERROR")
                    
                    # Check table against schema if available
                    schema = self._load_schema(table_name)
                    if schema and self.strict_schema_validation:
                        # Validate the table data against the schema
                        self._validate_against_schema(table_name, data, schema)
                        
                    return data
            except (json.JSONDecodeError, ValueError, IOError, OSError) as e:
                # Handle any file corruption or read error by trying to recover
                logger.error(f"Error reading table {table_name}: {str(e)}")
                logger.info(f"Attempting to auto-recover table {table_name}")
                
                # Try to recover from backup
                if self._auto_recover_from_backup(table_name):
                    logger.info(f"Successfully recovered table {table_name} from backup")
                    # Try to read the recovered table
                    return self.get_table(table_name)
                else:
                    logger.error(f"Failed to recover table {table_name}, returning empty table")
                    return []
            except StorageError as e:
                logger.error(f"Storage error for table {table_name}: {str(e)}")
                if self._auto_recover_from_backup(table_name):
                    logger.info(f"Successfully recovered table {table_name} from backup after storage error")
                    return self.get_table(table_name)
                return []
            except Exception as e:
                logger.error(f"Unexpected error reading table {table_name}: {str(e)}")
                return []
    
    def _update_schema(self, table_name, data):
        """Update or create a schema definition for a table based on its data."""
        if not data:
            return  # No data to infer schema from
            
        schema_path = os.path.join(self.SCHEMA_DIR, f"{table_name}_schema.json")
        
        # Detect fields and their types from the first record
        fields = {}
        nullable_fields = []
        
        # Combine all fields from all records to ensure we capture everything
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())
            
        # Determine the types for all fields
        for field in all_fields:
            # Check all records to determine field type
            field_types = set()
            field_nullable = False
            
            for record in data:
                if field not in record:
                    field_nullable = True
                    continue
                    
                value = record[field]
                if value is None:
                    field_nullable = True
                    continue
                    
                # Determine the type of this value
                if isinstance(value, str):
                    field_types.add("string")
                elif isinstance(value, (int, float)):
                    field_types.add("number")
                elif isinstance(value, bool):
                    field_types.add("boolean")
                elif isinstance(value, dict):
                    field_types.add("object")
                elif isinstance(value, list):
                    field_types.add("array")
                else:
                    field_types.add("unknown")
            
            # If the field is seen in all records
            if field_types:
                # If multiple types observed, use the most common or least restrictive
                if len(field_types) == 1:
                    fields[field] = next(iter(field_types))
                else:
                    # Default to string for mixed types
                    fields[field] = "string" 
                    
            # If a field can be null or missing in some records
            if field_nullable:
                nullable_fields.append(field)
        
        # Create the schema
        schema = {
            "fields": fields,
            "nullable_fields": nullable_fields,
            "created_at": time.time(),
            "updated_at": time.time()
        }
        
        # Save the schema
        try:
            with open(schema_path, "w", encoding="utf-8") as file:
                json.dump(schema, file, indent=4)
                
            # Update in-memory cache
            self.schema_cache[table_name] = schema
            
        except Exception as e:
            logger.error(f"Failed to save schema for {table_name}: {str(e)}")
    
    def _load_schema(self, table_name):
        """Load the schema for a table."""
        # Check in-memory cache first
        if table_name in self.schema_cache:
            return self.schema_cache[table_name]
            
        schema_path = os.path.join(self.SCHEMA_DIR, f"{table_name}_schema.json")
        
        if not os.path.exists(schema_path):
            return None
            
        try:
            with open(schema_path, "r", encoding="utf-8") as file:
                schema = json.load(file)
                
            # Cache the schema
            self.schema_cache[table_name] = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to load schema for {table_name}: {str(e)}")
            return None
    
    def _auto_recover_from_backup(self, table_name):
        """Automatically recover a table from the latest backup."""
        # Prevent infinite recovery loops
        if table_name not in self.recovery_attempts:
            self.recovery_attempts[table_name] = 0
            
        self.recovery_attempts[table_name] += 1
        
        if self.recovery_attempts[table_name] > 3:
            logger.error(f"Too many recovery attempts for table {table_name}")
            return False
            
        try:
            # Find the latest backup
            backups = self.list_backups(table_name)
            if not backups:
                logger.warning(f"No backups found for table {table_name}")
                return False
                
            # Get the most recent backup
            latest_backup = backups[0]
            backup_path = os.path.join(self.BACKUP_DIR, latest_backup["filename"])
            
            # Verify backup integrity
            try:
                self._verify_table_integrity(backup_path)
            except Exception as e:
                logger.error(f"Backup integrity check failed: {str(e)}")
                # If the latest backup is corrupted, try the next one if available
                if len(backups) > 1:
                    logger.info(f"Trying older backup for {table_name}")
                    next_backup = backups[1]
                    backup_path = os.path.join(self.BACKUP_DIR, next_backup["filename"])
                    try:
                        self._verify_table_integrity(backup_path)
                    except Exception:
                        logger.error(f"Second backup also corrupted for {table_name}")
                        return False
                else:
                    return False
            
            # Copy the backup to the table location
            table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
            shutil.copy2(backup_path, table_path)
            
            logger.info(f"Restored table {table_name} from backup created at {latest_backup['formatted_time']}")
            return True
        except Exception as e:
            logger.error(f"Error during auto-recovery of {table_name}: {str(e)}")
            return False
                
    def table_exists(self, table_name):
        """Checks if a table exists in storage."""
        self._validate_table_name(table_name)
        table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
        
        if os.path.exists(table_path):
            # Verify table integrity
            try:
                self._verify_table_integrity(table_path)
                return True
            except Exception:
                # If table is corrupted, try to recover
                logger.warning(f"Table {table_name} exists but is corrupted, attempting recovery")
                if self._auto_recover_from_backup(table_name):
                    return True
                else:
                    # If recovery fails, table doesn't effectively exist
                    return False
        return False
        
    def delete_table(self, table_name):
        """Deletes a table with transaction awareness."""
        start_time = time.time()
        
        # Validate table name regardless of transaction status
        try:
            self._validate_table_name(table_name)
        except StorageError as e:
            # If in a transaction, fail early
            if self.transaction_manager.active_transaction:
                raise e
                
        # Check if table exists before queueing in a transaction
        if self.transaction_manager.active_transaction:
            if not self.table_exists(table_name):
                raise StorageError(f"Cannot delete non-existent table '{table_name}' in a transaction", 
                                 table_name, "TABLE_NOT_FOUND")
            
            # Queue the operation if a transaction is active
            self.transaction_manager.log_operation({
                "operation": "delete",
                "table_name": table_name
            })
            self._log_performance("Queue delete", table_name, start_time)
            return True

        # Direct delete for non-transactional operations
        result = self._direct_delete_table(table_name)
        self._log_performance("Delete", table_name, start_time)
        return result
        
    def _direct_delete_table(self, table_name):
        """Internal method to delete table directly (used by transaction commit)."""
        start_time = time.time()
        
        self._validate_table_name(table_name)
        table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
        schema_path = os.path.join(self.SCHEMA_DIR, f"{table_name}_schema.json")
        
        # Create a backup before deleting
        backup_successful = self.backup_table(table_name)
        if not backup_successful:
            logger.warning(f"Could not create backup before deleting table {table_name}")
            
        # Special backup for deletions
        delete_backup_path = os.path.join(self.BACKUP_DIR, f"{table_name}_deleted_{int(time.time())}.json")
        
        # Get lock for this table
        lock = self._get_file_lock(table_name)
        
        with lock:
            if not os.path.exists(table_path):
                self._log_performance("Delete (table not found)", table_name, start_time)
                return False
                
            try:
                # Create an additional deletion-specific backup
                shutil.copy2(table_path, delete_backup_path)
                
                # Delete the table file
                os.remove(table_path)
                
                # Also delete the schema file if it exists
                if os.path.exists(schema_path):
                    os.remove(schema_path)
                    # Remove from schema cache
                    if table_name in self.schema_cache:
                        del self.schema_cache[table_name]
                
                self._log_performance("Direct delete", table_name, start_time)
                return True
            except Exception as e:
                logger.error(f"Failed to delete table {table_name}: {str(e)}")
                self._log_performance("Failed delete", table_name, start_time)
                raise StorageError(f"Failed to delete table: {str(e)}", table_name, "DELETE_ERROR")
    
    def restore_backup(self, table_name, backup_time=None):
        """Restores a table from a backup.
        If backup_time is provided, restores from that specific backup,
        otherwise restores the most recent backup."""
        self._validate_table_name(table_name)
        
        backup_files = [
            f for f in os.listdir(self.BACKUP_DIR)
            if f.startswith(f"{table_name}_") and f.endswith(".json") and not "deleted" in f
        ]
        
        if not backup_files:
            return False, "No backups found for this table"
            
        # Sort by timestamp (assuming format: tablename_timestamp.json)
        backup_files.sort(reverse=True)
        
        # If a specific backup time is requested, find that backup
        if backup_time:
            matching_backup = None
            for backup in backup_files:
                try:
                    timestamp = int(backup.split('_')[1].split('.')[0])
                    if timestamp == backup_time:
                        matching_backup = backup
                        break
                except (IndexError, ValueError):
                    continue
            
            if not matching_backup:
                return False, f"No backup found for timestamp {backup_time}"
            
            backup_to_restore = matching_backup
        else:
            # Otherwise use the most recent backup
            backup_to_restore = backup_files[0]
        
        # Get the backup file
        backup_path = os.path.join(self.BACKUP_DIR, backup_to_restore)
        
        try:
            # Verify backup integrity
            self._verify_table_integrity(backup_path)
            
            # Read the backup data
            with open(backup_path, "r", encoding="utf-8") as file:
                data_with_checksum = json.load(file)
                
                # Extract data from checksum-protected object
                if isinstance(data_with_checksum, dict) and "rows" in data_with_checksum:
                    data = data_with_checksum["rows"]
                else:
                    # Legacy format without checksums
                    data = data_with_checksum
                
            # Validate data format
            if not isinstance(data, list):
                return False, "Invalid backup format"
                
            # Save the data to restore the table
            self.save_table(table_name, data)
            
            # Get timestamp from filename for the message
            try:
                timestamp = int(backup_to_restore.split('_')[1].split('.')[0])
                restore_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            except (IndexError, ValueError):
                restore_time = "unknown time"
                
            return True, f"Restored table {table_name} from backup created at {restore_time}"
        except Exception as e:
            logger.error(f"Failed to restore backup for {table_name}: {str(e)}")
            return False, f"Failed to restore backup: {str(e)}"
                
    def list_tables(self):
        """Returns a list of all tables in the database."""
        try:
            if not os.path.exists(self.DATA_DIR):
                return []
                
            tables = []
            for f in os.listdir(self.DATA_DIR):
                if f.endswith(".json") and os.path.isfile(os.path.join(self.DATA_DIR, f)):
                    table_name = os.path.splitext(f)[0]
                    # Verify table integrity
                    try:
                        table_path = os.path.join(self.DATA_DIR, f)
                        self._verify_table_integrity(table_path)
                        tables.append(table_name)
                    except Exception:
                        # If table is corrupted, try to recover
                        logger.warning(f"Table {table_name} is corrupted, attempting recovery")
                        if self._auto_recover_from_backup(table_name):
                            tables.append(table_name)
            
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            raise StorageError(f"Failed to list tables: {str(e)}", None, "LIST_ERROR")
    
    def list_backups(self, table_name=None):
        """Lists all available backups, optionally filtered by table name."""
        try:
            if not os.path.exists(self.BACKUP_DIR):
                return []
                
            backups = []
            for f in os.listdir(self.BACKUP_DIR):
                if not f.endswith(".json"):
                    continue
                    
                if table_name and not f.startswith(f"{table_name}_"):
                    continue
                    
                # Extract timestamp from filename
                try:
                    parts = f.split('_')
                    if len(parts) >= 2:
                        table = parts[0]
                        timestamp_str = parts[1].split('.')[0]
                        if timestamp_str.isdigit():
                            timestamp = int(timestamp_str)
                            is_deleted = "deleted" in f
                            backups.append({
                                "table": table,
                                "timestamp": timestamp,
                                "formatted_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp)),
                                "filename": f,
                                "is_deleted": is_deleted
                            })
                except Exception:
                    # Skip files with unparseable names
                    continue
                    
            # Sort by timestamp, newest first
            return sorted(backups, key=lambda x: x["timestamp"], reverse=True)
        except Exception as e:
            logger.error(f"Failed to list backups: {str(e)}")
            raise StorageError(f"Failed to list backups: {str(e)}", None, "LIST_ERROR")
            
    def truncate_table(self, table_name):
        """Empties a table but keeps it in the database."""
        self._validate_table_name(table_name)
        
        # Create a backup before truncating
        self.backup_table(table_name)
        
        # Simply save an empty list to the table
        self.save_table(table_name, [])
        return True
        
    def validate_schema(self, table_name, record, schema=None):
        """Validates a record against a schema (if provided) or existing records."""
        if not record:
            return False
            
        if not isinstance(record, dict):
            raise StorageError("Record must be a dictionary", table_name, "TYPE_ERROR")
            
        # If explicit schema is provided, use it
        if schema:
            # Implement schema validation logic
            # This is a simplified version
            for field, field_type in schema.items():
                if field in record:
                    if field_type == "string" and not isinstance(record[field], str):
                        return False
                    elif field_type == "number" and not isinstance(record[field], (int, float)):
                        return False
                    elif field_type == "boolean" and not isinstance(record[field], bool):
                        return False
            return True
        
        # Otherwise use the stored schema if available
        stored_schema = self._load_schema(table_name)
        if stored_schema:
            try:
                self._validate_against_schema(table_name, [record], stored_schema)
                return True
            except StorageError:
                return False
            
        # If no schema, infer from existing data
        existing_data = self.get_table(table_name)
        if not existing_data:
            # No existing records, so any schema is valid
            return True
            
        # Check that the record has at least the same fields as the first record
        first_record = existing_data[0]
        for field in first_record:
            if field not in record:
                return False
                
        return True
        
    def get_table_schema(self, table_name):
        """Get the schema for a table."""
        schema = self._load_schema(table_name)
        if schema:
            return schema
            
        # If no schema exists, try to create one from the data
        data = self.get_table(table_name)
        if data:
            self._update_schema(table_name, data)
            return self._load_schema(table_name)
            
        return None
        
    def get_database_info(self):
        """Get information about the database."""
        tables = self.list_tables()
        table_info = []
        
        total_rows = 0
        total_size = 0
        
        for table_name in tables:
            table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
            schema = self._load_schema(table_name)
            
            # Get file size
            size = os.path.getsize(table_path) if os.path.exists(table_path) else 0
            
            # Get row count
            try:
                data = self.get_table(table_name)
                row_count = len(data)
                total_rows += row_count
                total_size += size
                
                table_info.append({
                    "name": table_name,
                    "rows": row_count,
                    "size_bytes": size,
                    "has_schema": schema is not None,
                    "fields": list(schema["fields"].keys()) if schema else []
                })
            except Exception as e:
                logger.error(f"Error getting info for table {table_name}: {str(e)}")
                table_info.append({
                    "name": table_name,
                    "rows": "ERROR",
                    "size_bytes": size,
                    "has_schema": schema is not None,
                    "error": str(e)
                })
                
        return {
            "tables": table_info,
            "table_count": len(tables),
            "total_rows": total_rows,
            "total_size_bytes": total_size,
            "data_dir": os.path.abspath(self.DATA_DIR)
        }
        
    # Helper methods
    def _validate_table_name(self, table_name):
        """Validates that the table name is safe to use."""
        if not table_name or not isinstance(table_name, str):
            raise StorageError("Table name must be a non-empty string", table_name, "VALIDATION_ERROR")
            
        # Check for invalid characters or paths
        if ".." in table_name or "/" in table_name or "\\" in table_name:
            raise StorageError("Table name contains invalid characters", table_name, "VALIDATION_ERROR")
            
        # Ensure the table name is not too long
        if len(table_name) > 64:
            raise StorageError("Table name is too long (max 64 characters)", table_name, "VALIDATION_ERROR")
            
    def _recover_from_backup(self, table_name):
        """Attempts to recover table data from the latest backup."""
        backup_files = [
            f for f in os.listdir(self.BACKUP_DIR)
            if f.startswith(f"{table_name}_") and f.endswith(".json")
        ]
        
        if not backup_files:
            logger.warning(f"No backups found for table {table_name}")
            # No backups found, return empty table
            return []
            
        # Sort by timestamp (assuming format: tablename_timestamp.json)
        backup_files.sort(reverse=True)
        
        # Try each backup in order until we find a valid one
        for backup_file in backup_files:
            latest_backup = os.path.join(self.BACKUP_DIR, backup_file)
            
            try:
                # Verify backup integrity
                self._verify_table_integrity(latest_backup)
                
                with open(latest_backup, "r", encoding="utf-8") as file:
                    data_with_checksum = json.load(file)
                    
                    # Extract data
                    if isinstance(data_with_checksum, dict) and "rows" in data_with_checksum:
                        data = data_with_checksum["rows"]
                    else:
                        data = data_with_checksum
                        
                    if isinstance(data, list):
                        # Restore this backup
                        table_path = os.path.join(self.DATA_DIR, f"{table_name}.json")
                        shutil.copy2(latest_backup, table_path)
                        logger.info(f"Recovered table {table_name} from backup {backup_file}")
                        return data
            except Exception as e:
                logger.warning(f"Backup {backup_file} for {table_name} is corrupted: {str(e)}")
                continue
                
        # If all backups are corrupted, return empty table
        logger.error(f"All backups for {table_name} are corrupted")
        return []
            
    def _get_file_lock(self, table_name):
        """Gets or creates a lock for a specific table."""
        with self.lock_mutex:
            if table_name not in self.file_locks:
                self.file_locks[table_name] = threading.Lock()
            return self.file_locks[table_name]
            
    def cleanup_old_backups(self, max_age_days=7):
        """Cleans up backup files older than the specified age."""
        try:
            if not os.path.exists(self.BACKUP_DIR):
                return
                
            current_time = time.time()
            cleaned_count = 0
            
            for filename in os.listdir(self.BACKUP_DIR):
                file_path = os.path.join(self.BACKUP_DIR, filename)
                if os.path.isfile(file_path):
                    file_age = current_time - os.path.getmtime(file_path)
                    # Convert seconds to days
                    if file_age / (24 * 3600) > max_age_days:
                        os.remove(file_path)
                        cleaned_count += 1
                        
            logger.info(f"Cleaned up {cleaned_count} old backup files")
        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {str(e)}")
            
    def set_integrity_options(self, checksum_validation=True, strict_schema_validation=False):
        """Configure data integrity options."""
        self.checksum_validation = checksum_validation
        self.strict_schema_validation = strict_schema_validation
        logger.info(f"Storage integrity options updated: checksum_validation={checksum_validation}, " +
                   f"strict_schema_validation={strict_schema_validation}")
        return {
            "checksum_validation": self.checksum_validation,
            "strict_schema_validation": self.strict_schema_validation
        }

# Example Usage:
# storage = Storage()
# storage.save_table("users", [{"id": 1, "name": "Alice"}])
# print(storage.get_table("users"))
