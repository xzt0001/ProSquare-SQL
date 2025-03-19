"""
Query processor module to handle SQL operation execution.
This module provides handler functions for different SQL operations
that are called during transaction commits.
"""
import logging
import traceback

# Setup logger
logger = logging.getLogger('ProSquareSQL.QueryProcessor')

def process_insert(operation, storage):
    """Process an INSERT operation during transaction commit."""
    try:
        table_name = operation.get("table_name")
        data = operation.get("data", [])
        
        if not table_name:
            logger.error("INSERT operation missing table_name")
            return {"success": False, "error": "Missing table_name in INSERT operation"}
            
        if not isinstance(data, list):
            logger.error(f"INSERT operation data must be a list, got {type(data)}")
            return {"success": False, "error": f"Data must be a list, got {type(data)}"}
            
        result = storage._direct_save_table(table_name, data)
        return {"success": True, "affected_rows": len(data), "operation": "INSERT"}
    except Exception as e:
        error_msg = f"Error in INSERT operation: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return {"success": False, "error": error_msg}

def process_update(operation, storage):
    """Process an UPDATE operation during transaction commit."""
    try:
        table_name = operation.get("table_name")
        data = operation.get("data", [])
        where_clause = operation.get("where_clause")
        
        if not table_name:
            logger.error("UPDATE operation missing table_name")
            return {"success": False, "error": "Missing table_name in UPDATE operation"}
            
        if not isinstance(data, list):
            logger.error(f"UPDATE operation data must be a list, got {type(data)}")
            return {"success": False, "error": f"Data must be a list, got {type(data)}"}
            
        # For update we need to replace the entire table data
        result = storage._direct_save_table(table_name, data)
        return {"success": True, "affected_rows": len(data), "operation": "UPDATE", "where_clause": where_clause}
    except Exception as e:
        error_msg = f"Error in UPDATE operation: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return {"success": False, "error": error_msg}

def process_delete(operation, storage):
    """Process a DELETE operation during transaction commit."""
    try:
        table_name = operation.get("table_name")
        where_clause = operation.get("where_clause")
        
        if not table_name:
            logger.error("DELETE operation missing table_name")
            return {"success": False, "error": "Missing table_name in DELETE operation"}
            
        # For DELETE, we either delete the entire table or save a filtered version
        if not where_clause:
            # Delete entire table
            result = storage._direct_delete_table(table_name)
            return {"success": True, "operation": "DELETE", "affected_rows": "all"}
        else:
            # Where clause exists, so we've filtered the data in the transaction
            # and should have a replaced data array
            data = operation.get("data", [])
            result = storage._direct_save_table(table_name, data)
            return {"success": True, "operation": "DELETE", "affected_rows": len(data), "where_clause": where_clause}
    except Exception as e:
        error_msg = f"Error in DELETE operation: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return {"success": False, "error": error_msg}

def process_create(operation, storage):
    """Process a CREATE TABLE operation during transaction commit."""
    try:
        table_name = operation.get("table_name")
        schema = operation.get("schema", {})
        columns = operation.get("columns", [])
        
        if not table_name:
            logger.error("CREATE operation missing table_name")
            return {"success": False, "error": "Missing table_name in CREATE operation"}
        
        # Create an empty table
        storage._direct_save_table(table_name, [])
        
        # Store schema information if available
        if schema or columns:
            # Convert columns to schema format if needed
            if columns and not schema:
                schema = {"fields": {}}
                for col in columns:
                    col_name = col.get("name")
                    col_type = col.get("type", "string")
                    nullable = col.get("nullable", True)
                    schema["fields"][col_name] = col_type
                    if nullable:
                        if "nullable_fields" not in schema:
                            schema["nullable_fields"] = []
                        schema["nullable_fields"].append(col_name)
            
            # Store schema in storage system
            if hasattr(storage, "_update_schema") and callable(storage._update_schema):
                storage._update_schema(table_name, schema)
            else:
                logger.warning(f"Storage system doesn't support schema storage for table {table_name}")
        
        return {"success": True, "operation": "CREATE TABLE", "table_name": table_name, "has_schema": bool(schema)}
    except Exception as e:
        error_msg = f"Error in CREATE TABLE operation: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return {"success": False, "error": error_msg}

def process_drop(operation, storage):
    """Process a DROP TABLE operation during transaction commit."""
    try:
        table_name = operation.get("table_name")
        
        if not table_name:
            logger.error("DROP operation missing table_name")
            return {"success": False, "error": "Missing table_name in DROP operation"}
            
        if not storage.table_exists(table_name):
            logger.warning(f"Attempted to drop non-existent table: {table_name}")
            return {"success": False, "error": f"Table {table_name} does not exist"}
            
        result = storage._direct_delete_table(table_name)
        
        # Also remove schema if storage system supports it
        schema_path = f"data/schemas/{table_name}_schema.json"
        import os
        if os.path.exists(schema_path):
            try:
                os.remove(schema_path)
                logger.debug(f"Removed schema file for table {table_name}")
            except Exception as schema_err:
                logger.warning(f"Could not remove schema for {table_name}: {schema_err}")
        
        return {"success": True, "operation": "DROP TABLE", "table_name": table_name}
    except Exception as e:
        error_msg = f"Error in DROP TABLE operation: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return {"success": False, "error": error_msg}

def process_select(operation, storage):
    """Process a SELECT operation during transaction commit.
    
    This is primarily for read-only queries, but in transaction context
    it can be used for consistency checks or to ensure tables exist.
    """
    try:
        table_name = operation.get("table_name")
        columns = operation.get("columns", [])
        where_clause = operation.get("where_clause")
        order_by = operation.get("order_by")
        limit_offset = operation.get("limit_offset")
        
        if not table_name:
            logger.error("SELECT operation missing table_name")
            return {"success": False, "error": "Missing table_name in SELECT operation"}
            
        # Check if the table exists
        if not storage.table_exists(table_name):
            logger.warning(f"SELECT on non-existent table: {table_name}")
            return {"success": False, "error": f"Table {table_name} does not exist"}
        
        # For transaction validation purposes, we can just check the table exists
        # But we could also retrieve and filter data if needed for consistency checks
        data = []
        if operation.get("validate_data", False):
            data = storage.get_table(table_name)
            
            # Apply filters if needed (simplified version)
            if where_clause and data:
                # Note: Real filtering would be implemented here
                logger.debug(f"Data validation with WHERE clause for {table_name}")
        
        return {
            "success": True, 
            "operation": "SELECT", 
            "table_name": table_name,
            "row_count": len(data) if data else None,
            "validation": operation.get("validate_data", False)
        }
    except Exception as e:
        error_msg = f"Error in SELECT operation: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return {"success": False, "error": error_msg} 