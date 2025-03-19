import re
import logging

# Configure logging
logger = logging.getLogger('ProSquareSQL')

class SQLParserError(Exception):
    """Custom exception for SQL parsing errors with detailed context."""
    def __init__(self, message, query=None, position=None, error_type="SYNTAX_ERROR"):
        self.message = message
        self.query = query
        self.position = position
        self.error_type = error_type
        super().__init__(self.message)
        
    def __str__(self):
        error_msg = f"SQL {self.error_type}: {self.message}"
        if self.query and self.position:
            # Show the query and position where the error occurred
            error_msg += f"\nAt position {self.position} in query: {self.query[:self.position]}→{self.query[self.position:]}"
        elif self.query:
            error_msg += f"\nIn query: {self.query}"
        return error_msg

class SQLParser:
    def __init__(self):
        """Initialize the SQL parser with basic SQL syntax support."""
        self.commands = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP"]
        
        # JOIN types
        self.join_types = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN", "JOIN"]
        
        # Regex patterns for validating SQL syntax - Optimized versions
        # Using less backtracking and more efficient patterns
        self.regex_patterns = {
            "SELECT": r"^\s*SELECT\s+([^FROM]+?)\s+FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?(?:\s+(?:(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?(?:\s+ON\s+([^WHERE|ORDER|LIMIT]+?))?)?(?:\s+WHERE\s+([^ORDER|LIMIT]+?))?(?:\s+GROUP\s+BY\s+([^HAVING|ORDER|LIMIT]+?))?(?:\s+HAVING\s+([^ORDER|LIMIT]+?))?(?:\s+ORDER\s+BY\s+([^LIMIT]+?))?(?:\s+LIMIT\s+(\d+))?\s*$",
            "INSERT": r"^\s*INSERT\s+INTO\s+(\w+)(?:\s*\(([^)]+)\))?\s+VALUES\s*\(([^)]+)\)\s*$",
            "UPDATE": r"^\s*UPDATE\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+SET\s+([^WHERE]+?)(?:\s+WHERE\s+(.+?))?\s*$",
            "DELETE": r"^\s*DELETE\s+FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?(?:\s+WHERE\s+(.+?))?\s*$",
            "CREATE": r"^\s*CREATE\s+TABLE\s+(\w+)(?:\s*\(([^)]+)\))?\s*$",
            "DROP": r"^\s*DROP\s+TABLE\s+(\w+)\s*$"
        }
        
        # Define mapping of error patterns to helpful error messages - Enhanced with more specific contexts
        self.error_patterns = {
            "missing_from": (r"SELECT\s+(.+?)(?:\s+WHERE|\s+ORDER|\s+LIMIT|\s*$)", 
                            "Missing FROM clause in SELECT statement. You need to specify which table to query from."),
            "missing_where_condition": (r"WHERE\s*(?:ORDER|LIMIT|$)",
                                     "WHERE clause is missing a condition. You must provide a filtering condition after WHERE."),
            "missing_set": (r"UPDATE\s+(\w+)(?:\s+(?:AS\s+)?\w+)?\s+(?:WHERE|ORDER|LIMIT|$)",
                           "Missing SET clause in UPDATE statement. You need to specify which columns to update with SET."),
            "missing_values": (r"INSERT\s+INTO\s+(\w+)(?:\s*\([^)]+\))?\s*(?:WHERE|ORDER|LIMIT|$)",
                             "Missing VALUES clause in INSERT statement. You need to provide values to insert."),
            "unbalanced_parentheses": (r"\(([^)]*$)",
                                      "Unbalanced parentheses - missing closing parenthesis. Check if all your parentheses are properly paired."),
            "invalid_comparison": (r"WHERE\s+\w+\s*(?!=>|<=|<>|!=|=|>|<|IS|IS NOT)",
                                 "Invalid comparison operator in WHERE clause. Use operators like =, <, >, <=, >=, <>, !=, IS, or IS NOT."),
            "missing_table_name": (r"(FROM|UPDATE|INTO|TABLE)\s*(?:WHERE|SET|VALUES|\(|$)",
                                  "Missing table name after '{0}'. You must specify which table to operate on."),
            "missing_join_on": (r"(INNER|LEFT|RIGHT|FULL)\s+JOIN\s+\w+(?:\s+(?:AS\s+)?\w+)?\s+(?:WHERE|ORDER|LIMIT|$)",
                              "Missing ON clause in {0} JOIN statement. JOIN operations require an ON condition to match rows."),
            "invalid_join_syntax": (r"JOIN\s*(?:WHERE|ORDER|LIMIT|$)",
                                 "Invalid JOIN syntax - missing table name. You must specify which table to join with."),
            "invalid_on_condition": (r"ON\s+(?![\w.]+\s*=\s*[\w.]+)(?:[^WHERE|ORDER|LIMIT]+)?",
                                   "Invalid ON condition syntax. JOIN conditions must use the format: table1.column = table2.column"),
            "ambiguous_column_reference": (r"ON\s+(\w+)(?:\s*=\s*)(\w+)",
                                         "Ambiguous column reference in JOIN condition. Use table.column notation to avoid ambiguity: {0} = {1}"),
            "missing_group_by": (r"SELECT\s+.+\s+(SUM|AVG|COUNT|MIN|MAX)\s*\(.+\)\s+FROM\s+.+(?!GROUP\s+BY)",
                                 "Missing GROUP BY clause when using aggregation functions. You need to specify how to group the results."),
            "invalid_having": (r"HAVING\s*(?:ORDER|LIMIT|$)",
                               "HAVING clause is missing a condition. You must provide a filtering condition after HAVING.")
        }
        
        # Track aliases for table references
        self.table_aliases = {}

    def parse(self, query):
        """Parses an SQL query and returns the operation type and components with logging for debugging."""
        # Clean the query
        original_query = query
        query = query.strip()
        
        # Log the original query
        logger.debug(f"Parsing query: {original_query}")
        
        # Clear any previous aliases
        self.table_aliases = {}
        
        # Remove trailing semicolon if present
        if query.endswith(';'):
            query = query[:-1]
            
        # Remove comments - using string operations for efficiency
        cleaned_query = []
        for line in query.split('\n'):
            comment_pos = line.find('--')
            if comment_pos >= 0:
                line = line[:comment_pos]
            if line.strip():
                cleaned_query.append(line)
        query = ' '.join(cleaned_query)
        
        if not query.strip():
            raise SQLParserError("Empty query. Please provide a valid SQL statement.", original_query, error_type="EMPTY_QUERY")
            
        # Convert to uppercase for command detection, but preserve original for parsing
        query_upper = query.upper()
        
        # Log the cleaned query
        logger.debug(f"Cleaned query: {query}")
        
        # Identify command - using string operations instead of regex for speed
        command = None
        first_word = query_upper.split()[0] if query_upper.split() else ""
        if first_word in self.commands:
            command = first_word
                
        if not command:
            # Provide more specific error message based on what the query starts with
            raise SQLParserError(
                f"Unsupported SQL command: '{first_word}'. Must start with one of: {', '.join(self.commands)}. "
                f"Check the spelling of your command and ensure it's a supported operation.",
                original_query,
                0,
                "UNSUPPORTED_COMMAND"
            )
        
        try:
            # Extract table aliases first to make them available for error checking
            self._extract_table_aliases(query, command)
            
            # Check for specific error patterns before regex validation
            for error_name, (pattern, message) in self.error_patterns.items():
                if re.search(pattern, query, re.IGNORECASE):
                    # Check if this actually represents an error based on the command
                    if self._is_pattern_an_error(error_name, command, query):
                        approx_position = self._find_error_position(query, pattern)
                        # Format the message if it contains placeholders
                        formatted_message = message
                        if '{0}' in message:
                            match = re.search(pattern, query, re.IGNORECASE)
                            if match and match.groups():
                                formatted_message = message.format(match.group(1))
                        raise SQLParserError(formatted_message, original_query, approx_position, error_name.upper())
            
            # Perform regex-based validation
            self._validate_with_regex(command, query, original_query)
            
            # Tokenize more intelligently
            tokens = self._tokenize_query(query)
                
            # Filter out empty tokens
            tokens = [token for token in tokens if token]
            
            result = {
                "command": command,
                "tokens": tokens,
                "original_query": original_query,
                "table_aliases": self.table_aliases  # Include table aliases in the result
            }
            
            # Extract regex match components and add them to the result
            match_components = self._extract_regex_components(command, query)
            if match_components:
                result["components"] = match_components
            
            # Validate syntax based on token structure
            self._validate_syntax(result, original_query)
            
            # For JOINs, validate the ON condition specifically
            if command == "SELECT":
                self._validate_join_on_condition(result, original_query)
                
            # Log the successful parsing
            logger.debug(f"Successfully parsed {command} query: {original_query[:50]}...")
                
            return result
            
        except SQLParserError:
            # Re-raise SQLParserError as is
            raise
        except Exception as e:
            # Convert generic exceptions to SQLParserError with context
            error_message = str(e)
            error_type = "PARSER_ERROR"
            
            # Try to determine a more specific error type based on the error message
            if "parentheses" in error_message.lower():
                error_type = "PARENTHESES_ERROR"
            elif "quote" in error_message.lower():
                error_type = "QUOTE_ERROR"
                
            raise SQLParserError(f"Error parsing SQL: {error_message}", original_query, 
                               None, error_type) from e
                               
    def _extract_table_aliases(self, query, command):
        """Extract table aliases from a query for better error reporting."""
        if command == "SELECT":
            # Extract main table and its potential alias
            match = re.search(r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                alias = match.group(2)
                if alias:
                    self.table_aliases[alias] = table_name
                    
            # Extract any joined tables and their aliases
            join_matches = re.finditer(r'JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
            for match in join_matches:
                join_table = match.group(1)
                join_alias = match.group(2)
                if join_alias:
                    self.table_aliases[join_alias] = join_table
        
        elif command == "UPDATE":
            # Extract update table and its potential alias
            match = re.search(r'UPDATE\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                alias = match.group(2)
                if alias:
                    self.table_aliases[alias] = table_name
                    
        elif command == "DELETE":
            # Extract delete table and its potential alias
            match = re.search(r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                alias = match.group(2)
                if alias:
                    self.table_aliases[alias] = table_name
                    
    def _validate_join_on_condition(self, query_data, original_query):
        """Validate the ON condition in JOIN clauses with improved error handling."""
        tokens = query_data["tokens"]
        
        # Check if this query has a JOIN
        tokens_upper = [t.upper() if isinstance(t, str) else t for t in tokens]
        if not any(join_type.split()[0] in tokens_upper or join_type.split()[-1] in tokens_upper for join_type in self.join_types):
            return  # No JOIN in this query
            
        # Find the ON clause
        if "ON" not in tokens_upper:
            # Already handled by missing_join_on pattern
            return
            
        on_idx = tokens_upper.index("ON")
        if on_idx + 1 >= len(tokens):
            raise SQLParserError(
                "Incomplete ON condition in JOIN clause. Expected format: table1.column = table2.column",
                original_query,
                self._find_token_position(original_query, "ON"),
                "JOIN_CONDITION_ERROR"
            )
            
        # Check for incomplete conditions like "ON table."
        if on_idx + 1 < len(tokens) and tokens[on_idx + 1].endswith(".") and (on_idx + 2 >= len(tokens) or tokens[on_idx + 2] == "="):
            table_name = tokens[on_idx + 1][:-1]  # Remove the trailing dot
            raise SQLParserError(
                f"Incomplete JOIN condition. Expected column reference after '{table_name}.'",
                original_query,
                self._find_token_position(original_query, tokens[on_idx + 1]),
                "INCOMPLETE_JOIN_CONDITION"
            )
            
        # Basic syntax check for the ON condition
        if on_idx + 3 >= len(tokens):
            raise SQLParserError(
                "Incomplete ON condition in JOIN clause. Expected format: table1.column = table2.column",
                original_query,
                self._find_token_position(original_query, "ON"),
                "JOIN_CONDITION_ERROR"
            )
            
        # Check basic syntax: ON left_expr = right_expr
        if tokens[on_idx + 2] != "=":
            raise SQLParserError(
                f"Invalid JOIN condition operator: '{tokens[on_idx + 2]}'. JOINs must use equality (=) operator.",
                original_query,
                self._find_token_position(original_query, tokens[on_idx + 2]),
                "JOIN_CONDITION_ERROR"
            )
            
        left_expr = tokens[on_idx + 1]
        right_expr = tokens[on_idx + 3]
        
        # Validate that both sides use table.column notation (or alias.column)
        if "." not in left_expr or "." not in right_expr:
            missing_dot = left_expr if "." not in left_expr else right_expr
            raise SQLParserError(
                f"JOIN conditions must use table.column or alias.column notation to avoid ambiguity. "
                f"Missing table reference in '{missing_dot}'",
                original_query,
                self._find_token_position(original_query, missing_dot),
                "AMBIGUOUS_COLUMN_REFERENCE"
            )
            
        # Validate that table/alias references are valid
        left_parts = left_expr.split(".")
        right_parts = right_expr.split(".")
        
        left_table_or_alias = left_parts[0]
        right_table_or_alias = right_parts[0]
        
        # Check if the references are valid tables or aliases
        all_table_refs = set(self.table_aliases.keys()) | set(self.table_aliases.values())
        
        if left_table_or_alias not in all_table_refs:
            raise SQLParserError(
                f"Unknown table or alias '{left_table_or_alias}' in JOIN condition. "
                f"Available tables and aliases: {', '.join(sorted(all_table_refs))}",
                original_query,
                self._find_token_position(original_query, left_expr),
                "UNKNOWN_TABLE_REFERENCE"
            )
            
        if right_table_or_alias not in all_table_refs:
            raise SQLParserError(
                f"Unknown table or alias '{right_table_or_alias}' in JOIN condition. "
                f"Available tables and aliases: {', '.join(sorted(all_table_refs))}",
                original_query,
                self._find_token_position(original_query, right_expr),
                "UNKNOWN_TABLE_REFERENCE"
            )
            
        # Ensure that the JOIN condition references different tables
        left_actual_table = self.table_aliases.get(left_table_or_alias, left_table_or_alias)
        right_actual_table = self.table_aliases.get(right_table_or_alias, right_table_or_alias)
        
        if left_actual_table == right_actual_table:
            raise SQLParserError(
                f"JOIN condition must reference different tables. Both sides reference '{left_actual_table}'.",
                original_query,
                self._find_token_position(original_query, "ON"),
                "INVALID_JOIN_CONDITION"
            )
            
    def _extract_regex_components(self, command, query):
        """Extracts components from query using regex patterns."""
        if command not in self.regex_patterns:
            return None
            
        pattern = self.regex_patterns[command]
        match = re.match(pattern, query, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return None
            
        # Extract components based on command type
        if command == "SELECT":
            # Updated to handle table aliases in SELECT
            # Check if query has a JOIN clause
            join_info = self._extract_join_clause(query)
            
            # Extract aggregation functions
            columns = match.group(1).strip() if match.group(1) else "*"
            aggregations = re.findall(r"(SUM|AVG|COUNT|MIN|MAX)\s*\(.+?\)", columns, re.IGNORECASE)

            # Ensure GROUP BY is present if aggregations are used
            group_by = match.group(8).strip() if match.group(8) else None
            if aggregations and not group_by:
                raise SQLParserError(
                    "Missing GROUP BY clause when using aggregation functions. You need to specify how to group the results.",
                    query,
                    None,
                    "MISSING_GROUP_BY"
                )

            return {
                "columns": columns,
                "aggregations": aggregations,
                "table": match.group(2).strip(),
                "table_alias": match.group(3).strip() if match.group(3) else None,
                "join": join_info,
                "where": match.group(7).strip() if match.group(7) else None,
                "group_by": group_by,
                "having": match.group(9).strip() if match.group(9) else None,
                "order_by": match.group(10).strip() if match.group(10) else None,
                "limit": int(match.group(11)) if match.group(11) else None
            }
        elif command == "INSERT":
            return {
                "table": match.group(1).strip(),
                "columns": [col.strip() for col in match.group(2).split(',')] if match.group(2) else None,
                "values": [val.strip() for val in match.group(3).split(',')]
            }
        elif command == "UPDATE":
            return {
                "table": match.group(1).strip(),
                "table_alias": match.group(2).strip() if match.group(2) else None,
                "set": match.group(3).strip(),
                "where": match.group(4).strip() if match.group(4) else None
            }
        elif command == "DELETE":
            return {
                "table": match.group(1).strip(),
                "table_alias": match.group(2).strip() if match.group(2) else None,
                "where": match.group(3).strip() if match.group(3) else None
            }
        elif command == "CREATE":
            return {
                "table": match.group(1).strip(),
                "columns": match.group(2).strip() if match.group(2) else None
            }
        elif command == "DROP":
            return {
                "table": match.group(1).strip()
            }
            
        return None
    
    def _extract_join_clause(self, query):
        """Extract JOIN type, tables, and conditions from a query with improved efficiency."""
        # Define all supported JOIN types
        join_types = {
            "INNER JOIN": "INNER JOIN", 
            "LEFT JOIN": "LEFT JOIN", 
            "RIGHT JOIN": "RIGHT JOIN", 
            "FULL JOIN": "FULL JOIN", 
            "CROSS JOIN": "CROSS JOIN", 
            "JOIN": "INNER JOIN",  # Default JOIN is INNER JOIN
            "LEFT OUTER JOIN": "LEFT JOIN",  # Map alias to standard type
            "RIGHT OUTER JOIN": "RIGHT JOIN",  # Map alias to standard type
            "FULL OUTER JOIN": "FULL JOIN"  # Map alias to standard type
        }
        
        # More efficient single-pass pattern to detect table aliases and join type
        # This captures all components in a single regex
        join_pattern = r"(\w+(?:\s+\w+)*\s+JOIN)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?(?:\s+ON\s+(.+?))?(?=\s+(?:\w+\s+JOIN|\s+WHERE|\s+ORDER|\s+LIMIT|\s*$))"
        
        matches = list(re.finditer(join_pattern, query, re.IGNORECASE))
        
        if not matches:
            return None
        
        # Track joined tables to detect redundant joins
        joined_tables = set()
        
        # For now, we'll just handle the first JOIN clause
        # In the future, this could be extended to handle multiple JOINs
        match = matches[0]
        
        # Get the JOIN type and normalize it
        join_type = match.group(1).upper()
        
        # Validate and normalize the join type in a single step
        if join_type not in join_types:
            raise SQLParserError(
                f"Unrecognized JOIN type: '{join_type}'. Expected one of: {', '.join(sorted(join_types.keys()))}",
                query,
                match.start(),
                "UNSUPPORTED_JOIN_TYPE"
            )
            
        # Normalize the join type (handles aliases like "JOIN" -> "INNER JOIN")
        normalized_join_type = join_types.get(join_type)
                
        # Extract joined table, alias and condition
        join_table = match.group(2)
        join_alias = match.group(3)  # This will capture any table alias
        join_condition = match.group(4)
        
        # Add the alias to table_aliases if it exists
        if join_alias:
            self.table_aliases[join_alias] = join_table
        
        # Check for redundant joins - if we already joined this table
        if join_table in joined_tables:
            logger.warning(f"Potentially redundant JOIN detected: Table '{join_table}' is joined multiple times")
        else:
            joined_tables.add(join_table)
            
        # For CROSS JOIN, no condition is needed
        if normalized_join_type == "CROSS JOIN" and not join_condition:
            return {
                "type": normalized_join_type,
                "table": join_table,
                "alias": join_alias,
                "condition": None
            }
            
        # For other JOINs, we need an ON condition
        if normalized_join_type != "CROSS JOIN" and not join_condition:
            raise SQLParserError(
                f"Missing ON condition in {normalized_join_type}. All non-CROSS JOIN operations require an ON condition.", 
                query, 
                match.end(), 
                "JOIN_SYNTAX_ERROR"
            )
        
        # Check for incorrect use of ON with CROSS JOIN
        if normalized_join_type == "CROSS JOIN" and join_condition:
            raise SQLParserError(
                f"CROSS JOIN cannot have an ON condition. Use INNER JOIN instead if you need filtering.", 
                query, 
                match.start() + match.group(0).upper().find("ON"), 
                "JOIN_SYNTAX_ERROR"
            )
            
        # Improved validation of ON condition with better error messages for edge cases
        if join_condition:
            # Check for incomplete ON conditions like "ON table."
            incomplete_condition = re.search(r'\bON\s+(\w+)\.(?!\w)', join_condition, re.IGNORECASE)
            if incomplete_condition:
                table_name = incomplete_condition.group(1)
                raise SQLParserError(
                    f"Incomplete JOIN condition. Expected column reference after '{table_name}.'",
                    query,
                    match.start() + match.group(0).upper().find("ON") + len("ON ") + join_condition.find(table_name + "."),
                    "INCOMPLETE_JOIN_CONDITION"
                )
            
            # Check for incomplete conditions like "ON table" without dot notation
            if not re.search(r'\w+\.\w+', join_condition):
                raise SQLParserError(
                    "Incomplete JOIN condition. Expected table.column notation in ON clause",
                    query,
                    match.start() + match.group(0).upper().find("ON") + len("ON "),
                    "INCOMPLETE_JOIN_CONDITION"
                )
            
            # Detect multiple conditions (AND/OR)
            has_multiple_conditions = re.search(r'\b(AND|OR)\b', join_condition, re.IGNORECASE)
            if has_multiple_conditions:
                # For now, we only support simple equality conditions
                raise SQLParserError(
                    f"Complex JOIN conditions with AND/OR are not supported yet. Please use a simple equality condition.",
                    query,
                    match.start() + match.group(0).upper().find("ON") + 2,
                    "COMPLEX_JOIN_CONDITION"
                )
            
            condition_parts = join_condition.split("=")
            if len(condition_parts) != 2:
                raise SQLParserError(
                    f"Invalid join condition format: {join_condition}. Expected format: table1.column = table2.column",
                    query,
                    match.end(),
                    "JOIN_CONDITION_ERROR"
                )
            
            left_expr = condition_parts[0].strip()
            right_expr = condition_parts[1].strip()
            
            # Check if condition uses table.column or alias.column notation
            # This is now more permissive to allow aliases
            uses_proper_notation = ("." in left_expr) and ("." in right_expr)
            if not uses_proper_notation:
                raise SQLParserError(
                    "JOIN conditions must use table.column or alias.column notation to avoid ambiguity. "
                    f"Expected format: table1.column = table2.column, got: {left_expr} = {right_expr}",
                    query,
                    match.start() + match.group(0).upper().find("ON") + 2,
                    "AMBIGUOUS_COLUMN_REFERENCE"
                )
                
            # Extract table/alias references
            try:
                left_table_or_alias = left_expr.split('.')[0].strip()
                right_table_or_alias = right_expr.split('.')[0].strip()
                
                # Check for invalid table references immediately
                all_table_refs = set(self.table_aliases.keys()) | set(self.table_aliases.values())
                
                if left_table_or_alias not in all_table_refs:
                    raise SQLParserError(
                        f"Unknown table or alias '{left_table_or_alias}' in JOIN condition. "
                        f"Available tables and aliases: {', '.join(sorted(all_table_refs))}",
                        query,
                        match.start() + match.group(0).find(left_expr),
                        "UNKNOWN_TABLE_REFERENCE"
                    )
                    
                if right_table_or_alias not in all_table_refs:
                    raise SQLParserError(
                        f"Unknown table or alias '{right_table_or_alias}' in JOIN condition. "
                        f"Available tables and aliases: {', '.join(sorted(all_table_refs))}",
                        query,
                        match.start() + match.group(0).find(right_expr),
                        "UNKNOWN_TABLE_REFERENCE"
                    )
                
            except IndexError:
                # This shouldn't happen due to earlier checks, but just in case
                raise SQLParserError(
                    f"Invalid join condition format: {join_condition}",
                    query,
                    match.end(),
                    "JOIN_CONDITION_ERROR"
                )
            
        return {
            "type": normalized_join_type,
            "table": join_table,
            "alias": join_alias,
            "condition": join_condition
        }
    
    def _tokenize_query(self, query):
        """More efficient tokenization method that replaces the previous in-parse tokenization."""
        tokens = []
        i = 0
        current_token = ""
        in_quotes = False
        quote_char = None
        
        while i < len(query):
            char = query[i]
            
            # Handle quoted strings
            if char in ["'", "\""]:
                if not in_quotes:
                    # Start of quoted string
                    if current_token:
                        tokens.append(current_token.strip())
                        current_token = ""
                    in_quotes = True
                    quote_char = char
                    current_token += char
                elif char == quote_char:
                    # End of quoted string
                    current_token += char
                    tokens.append(current_token)
                    current_token = ""
                    in_quotes = False
                else:
                    # Quote character inside different type of quotes
                    current_token += char
            # Handle special SQL characters
            elif char in [',', '(', ')', '=', '<', '>', '*'] and not in_quotes:
                if current_token:
                    tokens.append(current_token.strip())
                    current_token = ""
                tokens.append(char)
            # Handle whitespace
            elif char.isspace() and not in_quotes:
                if current_token:
                    tokens.append(current_token.strip())
                    current_token = ""
            else:
                current_token += char
            
            i += 1
            
        if current_token:
            tokens.append(current_token.strip())
            
        if in_quotes:
            raise SQLParserError("Unclosed quote in SQL query. Make sure all quotes are properly closed.", 
                               query, query.rfind(quote_char), "UNCLOSED_QUOTE")
                
        return tokens
    
    def _is_pattern_an_error(self, error_name, command, query):
        """Determine if a pattern match actually represents an error for the given command."""
        # Some patterns might be valid for specific commands but not others
        if error_name == "missing_from" and command != "SELECT":
            return False
        if error_name == "missing_set" and command != "UPDATE":
            return False
        if error_name == "missing_values" and command != "INSERT":
            return False
        
        # For WHERE conditions, we need more context
        if error_name == "missing_where_condition":
            # Check if WHERE exists in the query
            if "WHERE" not in query.upper():
                return False
            
            # Check if there's something after WHERE
            where_idx = query.upper().find("WHERE")
            after_where = query[where_idx + 5:].strip()
            return not after_where or after_where.upper().startswith(("ORDER", "LIMIT"))
            
        return True
        
    def _find_error_position(self, query, pattern):
        """Find the approximate position where an error pattern occurs."""
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            return match.start()
        return 0
    
    def _validate_with_regex(self, command, query, original_query):
        """Validates query syntax using regular expressions."""
        if command not in self.regex_patterns:
            return  # Skip for unsupported commands
            
        pattern = self.regex_patterns[command]
        match = re.match(pattern, query, re.IGNORECASE | re.DOTALL)
        
        if not match:
            # Provide helpful error messages for different commands
            error_messages = {
                "SELECT": "Invalid SELECT syntax. Expected format: SELECT columns FROM table [WHERE condition] [ORDER BY columns] [LIMIT n]. Check for missing clauses or incorrect ordering of clauses.",
                "INSERT": "Invalid INSERT syntax. Expected format: INSERT INTO table [(columns)] VALUES (values). Ensure all parentheses are balanced and VALUES keyword is present.",
                "UPDATE": "Invalid UPDATE syntax. Expected format: UPDATE table SET column=value [WHERE condition]. Make sure the SET clause is properly formatted.",
                "DELETE": "Invalid DELETE syntax. Expected format: DELETE FROM table [WHERE condition]. Verify the FROM keyword is present.",
                "CREATE": "Invalid CREATE syntax. Expected format: CREATE TABLE table [(column definitions)]. Check that column definitions are properly formatted.",
                "DROP": "Invalid DROP syntax. Expected format: DROP TABLE table. Ensure the TABLE keyword is present."
            }
            
            # Try to locate where the syntax error occurs
            error_position = self._locate_syntax_error(command, query)
            
            message = error_messages.get(command, f"Invalid {command} syntax")
            raise SQLParserError(message, original_query, error_position, "SYNTAX_ERROR")
    
    def _locate_syntax_error(self, command, query):
        """Try to locate approximately where a syntax error occurs."""
        query_upper = query.upper()
        
        # Check for common patterns based on the command
        if command == "SELECT":
            if "SELECT" in query_upper and "FROM" not in query_upper:
                return query_upper.find("SELECT") + 6  # After SELECT keyword
            if "FROM" in query_upper:
                return query_upper.find("FROM") + 4  # After FROM keyword
                
        elif command == "INSERT":
            if "INTO" not in query_upper:
                return query_upper.find("INSERT") + 6  # After INSERT keyword
            if "VALUES" not in query_upper:
                return query_upper.find("INTO") + 4  # After INTO keyword
                
        elif command == "UPDATE":
            if "SET" not in query_upper:
                return query_upper.find("UPDATE") + len("UPDATE " + self._extract_first_word_after(query, "UPDATE"))
            if "SET" in query_upper:
                return query_upper.find("SET") + 3  # After SET keyword
                
        # Default to the start of the query
        return 0
        
    def _extract_first_word_after(self, query, keyword):
        """Extract the first word after a keyword in a query."""
        match = re.search(f"{keyword}\\s+(\\w+)", query, re.IGNORECASE)
        if match:
            return match.group(1)
        return ""
    
    def _validate_syntax(self, query_data, original_query):
        """Validates basic syntax rules for different query types."""
        command = query_data["command"]
        tokens = query_data["tokens"]
        
        # Convert tokens to uppercase for easier keyword matching
        tokens_upper = [t.upper() if isinstance(t, str) and not (t.startswith("'") or t.startswith('"')) else t for t in tokens]
        
        error_position = 0  # Default position if we can't determine it better
        
        try:
            if command == "SELECT":
                # Check if FROM clause exists
                if "FROM" not in tokens_upper:
                    error_position = original_query.upper().find("SELECT") + 6
                    raise ValueError("Malformed SELECT query: Missing FROM clause or table name")
                    
                # Check for balanced parentheses
                if tokens.count("(") != tokens.count(")"):
                    error_position = self._find_unbalanced_parenthesis(original_query)
                    raise ValueError("Malformed SELECT query: Unbalanced parentheses")
                
                # Ensure JOIN appears after FROM and before WHERE/ORDER BY/LIMIT
                from_idx = tokens_upper.index("FROM")
                where_idx = tokens_upper.index("WHERE") if "WHERE" in tokens_upper else len(tokens)
                order_idx = tokens_upper.index("ORDER") if "ORDER" in tokens_upper else len(tokens)
                limit_idx = tokens_upper.index("LIMIT") if "LIMIT" in tokens_upper else len(tokens)
                
                # The earliest clause after FROM
                next_clause_idx = min(where_idx, order_idx, limit_idx)
                
                # Check if JOIN keywords appear in the right place
                join_keywords = ["JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
                for i, token in enumerate(tokens_upper):
                    if token in join_keywords and (i < from_idx + 1 or i >= next_clause_idx):
                        error_position = self._find_token_position(original_query, tokens[i])
                        raise ValueError(f"Malformed SELECT query: {token} must appear after FROM and before WHERE/ORDER BY/LIMIT")
                    
                # Check if WHERE clause is properly formed
                if "WHERE" in tokens_upper:
                    where_idx = tokens_upper.index("WHERE")
                    error_position = self._find_token_position(original_query, "WHERE")
                    # Must have at least field, operator, value after WHERE
                    if len(tokens) < where_idx + 4:
                        raise ValueError("Malformed SELECT query: Incomplete WHERE clause")
                        
                # Validate JOIN clause if present
                join_keywords = ["JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
                if any(keyword in tokens_upper for keyword in join_keywords):
                    # Find the JOIN keyword
                    join_idx = -1
                    for i, token in enumerate(tokens_upper):
                        if token in join_keywords or (i > 0 and tokens_upper[i-1] in ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"] and token == "JOIN"):
                            join_idx = i
                            break
                    
                    if join_idx != -1:
                        error_position = self._find_token_position(original_query, tokens[join_idx])
                        
                        # Check for table name after JOIN
                        if join_idx + 1 >= len(tokens):
                            raise ValueError(f"Malformed SELECT query: Missing table name after {tokens[join_idx]}")
                            
                        # For non-CROSS JOINs, check for ON clause
                        if tokens_upper[join_idx] != "CROSS" and "ON" not in tokens_upper[join_idx:]:
                            raise ValueError(f"Malformed SELECT query: Missing ON clause after {tokens[join_idx]}")
                            
                        # Check if ON clause is properly formed
                        if "ON" in tokens_upper[join_idx:]:
                            on_idx = tokens_upper.index("ON", join_idx)
                            error_position = self._find_token_position(original_query, "ON")
                            
                            # Must have a valid condition after ON
                            if len(tokens) < on_idx + 3:
                                raise ValueError("Malformed SELECT query: Incomplete ON condition")
                            
                            # Check if the ON condition uses table.column notation
                            if on_idx + 1 < len(tokens) and on_idx + 3 < len(tokens):
                                left_expr = tokens[on_idx + 1]
                                right_expr = tokens[on_idx + 3]
                                
                                if "." not in left_expr or "." not in right_expr:
                                    raise ValueError("Malformed JOIN condition: Must use table.column notation to avoid ambiguity")
                
                # Validate ORDER BY clause if present
                if "ORDER" in tokens_upper and "BY" in tokens_upper:
                    order_idx = tokens_upper.index("ORDER")
                    error_position = self._find_token_position(original_query, "ORDER")
                    if order_idx + 1 >= len(tokens_upper) or tokens_upper[order_idx + 1] != "BY":
                        raise ValueError("Malformed SELECT query: Invalid ORDER BY clause")
                    if order_idx + 2 >= len(tokens_upper):
                        raise ValueError("Malformed SELECT query: Missing column in ORDER BY clause")
                    
            elif command == "INSERT":
                # Check for INTO keyword
                if len(tokens) < 2 or tokens_upper[1] != "INTO":
                    error_position = original_query.upper().find("INSERT") + 6
                    raise ValueError("Malformed INSERT query: Expected 'INTO' after INSERT")
                    
                # Check for table name
                if len(tokens) < 3:
                    error_position = original_query.upper().find("INTO") + 4
                    raise ValueError("Malformed INSERT query: Missing table name")
                    
                # Check for VALUES keyword
                if "VALUES" not in tokens_upper:
                    error_position = original_query.upper().find(tokens[2]) + len(tokens[2])
                    raise ValueError("Malformed INSERT query: Missing VALUES clause")
                    
                # Check for balanced parentheses
                if tokens.count("(") != tokens.count(")"):
                    error_position = self._find_unbalanced_parenthesis(original_query)
                    raise ValueError("Malformed INSERT query: Unbalanced parentheses")
                    
                # If column names are specified, there should be an equal number of values
                if tokens.count("(") > 1:  # At least one for columns, one for values
                    # Count items in both parentheses groups
                    values_idx = tokens_upper.index("VALUES")
                    error_position = self._find_token_position(original_query, "VALUES")
                    col_start_idx = tokens.index("(")
                    col_end_idx = tokens.index(")")
                    val_start_idx = -1
                    for i in range(values_idx, len(tokens)):
                        if tokens[i] == "(":
                            val_start_idx = i
                            break
                    if val_start_idx == -1:
                        raise ValueError("Malformed INSERT query: Missing values parentheses")
                    val_end_idx = -1
                    for i in range(val_start_idx, len(tokens)):
                        if tokens[i] == ")":
                            val_end_idx = i
                            break
                    if val_end_idx == -1:
                        raise ValueError("Malformed INSERT query: Unclosed values parentheses")
                        
                    # Count columns and values (accounting for commas)
                    cols = [tokens[i] for i in range(col_start_idx + 1, col_end_idx) if tokens[i] != ","]
                    vals = [tokens[i] for i in range(val_start_idx + 1, val_end_idx) if tokens[i] != ","]
                    
                    if len(cols) != len(vals):
                        raise ValueError(f"Malformed INSERT query: Column count ({len(cols)}) does not match value count ({len(vals)})")
                
            elif command == "UPDATE":
                # Check for table name
                if len(tokens) < 2:
                    error_position = original_query.upper().find("UPDATE") + 6
                    raise ValueError("Malformed UPDATE query: Missing table name")
                    
                # Check for SET keyword
                if "SET" not in tokens_upper:
                    error_position = original_query.upper().find(tokens[1]) + len(tokens[1])
                    raise ValueError("Malformed UPDATE query: Missing SET clause")
                    
                # Basic check for column assignments
                set_idx = tokens_upper.index("SET")
                error_position = self._find_token_position(original_query, "SET") + 3
                if len(tokens) < set_idx + 3:  # Need at least SET column = value
                    raise ValueError("Malformed UPDATE query: Incomplete SET clause")
                    
                # Check for equal sign in SET clause
                has_equals = False
                for i in range(set_idx + 1, len(tokens)):
                    if tokens[i] == "=":
                        has_equals = True
                        break
                    if tokens_upper[i] == "WHERE":
                        break
                if not has_equals:
                    raise ValueError("Malformed UPDATE query: Missing assignment in SET clause")
                    
            elif command == "DELETE":
                # Check for FROM keyword
                if len(tokens) < 2 or tokens_upper[1] != "FROM":
                    error_position = original_query.upper().find("DELETE") + 6
                    raise ValueError("Malformed DELETE query: Expected 'FROM' after DELETE")
                    
                # Check for table name
                if len(tokens) < 3:
                    error_position = original_query.upper().find("FROM") + 4
                    raise ValueError("Malformed DELETE query: Missing table name")
                    
            elif command == "CREATE":
                # Check for TABLE keyword
                if len(tokens) < 2 or tokens_upper[1] != "TABLE":
                    error_position = original_query.upper().find("CREATE") + 6
                    raise ValueError("Malformed CREATE query: Expected 'TABLE' after CREATE")
                    
                # Check for table name
                if len(tokens) < 3:
                    error_position = original_query.upper().find("TABLE") + 5
                    raise ValueError("Malformed CREATE query: Missing table name")
                    
                # If columns are defined, check for balanced parentheses
                if "(" in tokens and tokens.count("(") != tokens.count(")"):
                    error_position = self._find_unbalanced_parenthesis(original_query)
                    raise ValueError("Malformed CREATE query: Unbalanced parentheses")
                    
            elif command == "DROP":
                # Check for TABLE keyword
                if len(tokens) < 2 or tokens_upper[1] != "TABLE":
                    error_position = original_query.upper().find("DROP") + 4
                    raise ValueError("Malformed DROP query: Expected 'TABLE' after DROP")
                    
                # Check for table name
                if len(tokens) < 3:
                    error_position = original_query.upper().find("TABLE") + 5
                    raise ValueError("Malformed DROP query: Missing table name")
        
        except ValueError as e:
            # Convert ValueError to SQLParserError with better context
            raise SQLParserError(str(e), original_query, error_position, "SYNTAX_ERROR")
            
    def _find_token_position(self, query, token):
        """Find the position of a token in the original query."""
        return query.upper().find(token.upper())
        
    def _find_unbalanced_parenthesis(self, query):
        """Find the position of an unbalanced parenthesis."""
        stack = []
        for i, char in enumerate(query):
            if char == '(':
                stack.append(i)
            elif char == ')':
                if not stack:
                    return i  # Closing parenthesis without an opening one
                stack.pop()
                
        # If we still have items in the stack, we have unclosed parentheses
        return stack[0] if stack else 0
    
    def extract_table_name(self, command, tokens):
        """Extract table name based on the command type."""
        try:
            if command == "SELECT":
                # Find FROM and take the next token as table name
                try:
                    from_index = [i for i, token in enumerate(tokens) if token.upper() == "FROM"][0]
                    return tokens[from_index + 1]
                except (IndexError, ValueError):
                    raise SQLParserError("Invalid SELECT syntax: Missing FROM clause or table name", None, None, "TABLE_NAME_ERROR")
            elif command == "INSERT":
                # Format: INSERT INTO table_name...
                try:
                    if tokens[1].upper() == "INTO":
                        return tokens[2]
                    raise SQLParserError("Invalid INSERT syntax: Expected 'INTO' after INSERT", None, None, "TABLE_NAME_ERROR")
                except IndexError:
                    raise SQLParserError("Invalid INSERT syntax", None, None, "TABLE_NAME_ERROR")
            elif command in ["UPDATE", "DELETE"]:
                # UPDATE table_name or DELETE FROM table_name
                try:
                    if command == "UPDATE":
                        return tokens[1]
                    else:  # DELETE
                        if tokens[1].upper() == "FROM":
                            return tokens[2]
                        raise SQLParserError("Invalid DELETE syntax: Expected 'FROM' after DELETE", None, None, "TABLE_NAME_ERROR")
                except IndexError:
                    raise SQLParserError(f"Invalid {command} syntax", None, None, "TABLE_NAME_ERROR")
            elif command == "CREATE" or command == "DROP":
                # CREATE TABLE table_name or DROP TABLE table_name
                try:
                    if tokens[1].upper() == "TABLE":
                        return tokens[2]
                    raise SQLParserError(f"Invalid {command} syntax: Expected 'TABLE' after {command}", None, None, "TABLE_NAME_ERROR")
                except IndexError:
                    raise SQLParserError(f"Invalid {command} syntax", None, None, "TABLE_NAME_ERROR")
            else:
                raise SQLParserError(f"Unsupported command for table name extraction: {command}", None, None, "UNSUPPORTED_COMMAND")
        except Exception as e:
            if isinstance(e, SQLParserError):
                raise
            # Convert other exceptions to SQLParserError
            raise SQLParserError(f"Error extracting table name: {str(e)}", None, None, "TABLE_NAME_ERROR") from e
            
    def is_query_valid(self, query):
        """Check if a query is syntactically valid without raising exceptions.
        Returns (is_valid, error_message, error_position)."""
        try:
            self.parse(query)
            return True, None, None
        except SQLParserError as e:
            # Return detailed error information
            return False, str(e), e.position
        except Exception as e:
            return False, f"Unexpected error: {str(e)}", None
            
    def get_detailed_error(self, query):
        """Attempt to parse a query and return detailed error information if it fails."""
        try:
            self.parse(query)
            return None  # No error
        except SQLParserError as e:
            error_info = {
                "is_valid": False,
                "message": e.message,
                "position": e.position,
                "error_type": e.error_type,
                "query": e.query
            }
            if e.position is not None:
                # Add context around the error
                context_before = e.query[:e.position] if e.query else ""
                context_after = e.query[e.position:] if e.query else ""
                error_info["context"] = {
                    "before": context_before[-20:] if len(context_before) > 20 else context_before,
                    "after": context_after[:20] if len(context_after) > 20 else context_after
                }
            return error_info
        except Exception as e:
            return {
                "is_valid": False,
                "message": f"Unexpected error: {str(e)}",
                "error_type": "UNKNOWN_ERROR",
                "query": query
            }

# Example Usage:
# parser = SQLParser()
# print(parser.parse("SELECT * FROM users WHERE age > 25;"))
