from engine.sql_parser import SQLParser
from engine.query_executor import QueryExecutor
import time
import os
import sys
import hashlib
import json
import logging
import bcrypt
import hmac
import keyring
import secrets
import base64
from datetime import datetime
from base64 import b64encode, b64decode
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from utils.logger import log_event, verify_log_integrity

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
        
        # Authentication and permission vars
        self.current_user = None
        self.authenticated = False
        self.users_file = "users.json"
        self.permissions_file = "permissions.json"
        self.keyring_service = "ProSquareSQL"  # Service name for keyring
        
        # Setup logging
        self.setup_logging()
        
        # Initialize encryption
        self._initialize_encryption()
        
        # Initialize user and permissions system
        self.initialize_user_system()

    def _initialize_encryption(self):
        """Initialize encryption using system keyring for secure storage with key derivation"""
        self.keyring_disabled = False  # Flag to track if keyring is disabled
        
        try:
            # Generate a static salt for key derivation - in production this should be securely stored
            encryption_salt = b'ProSquareSQL_Encryption_Salt'
            hmac_salt = b'ProSquareSQL_HMAC_Salt'
            
            # Test if keyring is available
            test_key = "test_keyring_availability"
            try:
                keyring.set_password(self.keyring_service, test_key, "test_value")
                keyring.delete_password(self.keyring_service, test_key)
            except Exception as e:
                self.keyring_disabled = True
                log_event(f"SECURITY WARNING: System keyring is disabled or unavailable: {e}", "WARNING")
                log_event(f"Falling back to less secure file-based storage", "WARNING")
                self._initialize_encryption_fallback()
                return
            
            # Try to get the base key from the keyring
            base_key = keyring.get_password(self.keyring_service, "base_key")
            
            # If the base key doesn't exist, generate a new one and store it
            if base_key is None:
                # Generate a strong random base key
                base_key = secrets.token_hex(32)
                keyring.set_password(self.keyring_service, "base_key", base_key)
                log_event(f"Generated and stored new base key in system keyring")
            
            # Convert the base key to bytes
            base_key_bytes = base_key.encode('utf-8')
            
            # Derive encryption key using PBKDF2
            kdf_encryption = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,  # 32 bytes for Fernet
                salt=encryption_salt,
                iterations=100000,
            )
            encryption_key = base64.urlsafe_b64encode(kdf_encryption.derive(base_key_bytes))
            
            # Create Fernet cipher with the derived key
            self.cipher = Fernet(encryption_key)
            
            # Derive HMAC key using PBKDF2 with different salt
            kdf_hmac = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=hmac_salt,
                iterations=100000,
            )
            hmac_key = kdf_hmac.derive(base_key_bytes)
            
            # Set the derived HMAC key
            self.hmac_key = hmac_key
            
            log_event(f"Successfully initialized encryption with key derivation")
            
        except Exception as e:
            self.keyring_disabled = True
            log_event(f"Error initializing encryption: {e}", "ERROR")
            log_event(f"SECURITY WARNING: Falling back to less secure file-based storage", "WARNING")
            print(f"Error initializing encryption: {e}")
            # Fallback to file-based storage if keyring is not available
            self._initialize_encryption_fallback()
    
    def _initialize_encryption_fallback(self):
        """Fallback encryption method using file storage if keyring is unavailable"""
        log_event(f"Using fallback encryption with file storage - this is less secure!", "WARNING")
        base_key_file = ".base_key"
        
        # Generate a static salt for key derivation
        encryption_salt = b'ProSquareSQL_Encryption_Salt'
        hmac_salt = b'ProSquareSQL_HMAC_Salt'
        
        if not os.path.exists(base_key_file):
            # Generate a new base key
            base_key = secrets.token_bytes(32)
            with open(base_key_file, 'wb') as f:
                f.write(base_key)
            log_event(f"Generated new base key (fallback method)")
        else:
            # Load existing base key
            with open(base_key_file, 'rb') as f:
                base_key = f.read()
        
        # Derive encryption key using PBKDF2
        kdf_encryption = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=encryption_salt,
            iterations=100000,
        )
        encryption_key = base64.urlsafe_b64encode(kdf_encryption.derive(base_key))
        
        # Create Fernet cipher with the key
        self.cipher = Fernet(encryption_key)
        
        # Derive HMAC key using PBKDF2 with different salt
        kdf_hmac = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=hmac_salt,
            iterations=100000,
        )
        hmac_key = kdf_hmac.derive(base_key)
        
        # Set the derived HMAC key
        self.hmac_key = hmac_key
        log_event(f"Initialized encryption with key derivation (fallback method)")
    
    def _hmac_password(self, hashed_password):
        """Apply HMAC to an already hashed password for extra security"""
        if isinstance(hashed_password, str):
            hashed_password = hashed_password.encode('utf-8')
            
        # Create HMAC using SHA-256
        h = hmac.new(self.hmac_key, hashed_password, hashlib.sha256)
        return b64encode(h.digest()).decode('utf-8')
    
    def _verify_hmac_password(self, password, stored_hmac, stored_hash):
        """Verify a password against stored HMAC and hash"""
        if isinstance(password, str):
            password = password.encode('utf-8')
            
        # Convert stored hash to bytes if it's a string
        if isinstance(stored_hash, str):
            stored_hash = stored_hash.encode('utf-8')
            
        # First check if the password matches using bcrypt
        if bcrypt.checkpw(password, stored_hash):
            # Then verify the HMAC
            expected_hmac = self._hmac_password(stored_hash)
            return hmac.compare_digest(expected_hmac, stored_hmac)
        
        return False
        
    def _encrypt_data(self, data):
        """Encrypt data before saving to disk"""
        json_str = json.dumps(data)
        encrypted_data = self.cipher.encrypt(json_str.encode('utf-8'))
        return encrypted_data
    
    def _decrypt_data(self, encrypted_data):
        """Decrypt data loaded from disk"""
        try:
            decrypted_data = self.cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode('utf-8'))
        except Exception as e:
            log_event(f"Failed to decrypt data: {e}", "ERROR")
            return {}
    
    def _save_encrypted_json(self, file_path, data):
        """Save data as encrypted JSON to file"""
        encrypted_data = self._encrypt_data(data)
        with open(file_path, 'wb') as f:
            f.write(encrypted_data)
    
    def _load_encrypted_json(self, file_path):
        """Load and decrypt JSON data from file"""
        if not os.path.exists(file_path):
            return {}
            
        with open(file_path, 'rb') as f:
            encrypted_data = f.read()
            
        if not encrypted_data:
            return {}
            
        return self._decrypt_data(encrypted_data)

    def setup_logging(self):
        """Setup logger reference for backward compatibility"""
        self.logger = logging.getLogger("security")

    def verify_log_integrity(self):
        """Verify the integrity of the security log with backup verification"""
        result, message = verify_log_integrity()
        
        # Store the result in keyring for additional security
        try:
            keyring.set_password(self.keyring_service, "last_log_check_result", str(result))
            keyring.set_password(self.keyring_service, "last_log_check_time", datetime.now().isoformat())
        except Exception as e:
            # Continue even if keyring fails
            pass
        
        log_event(f"Log integrity check: {message}")
        return result, message
        
    def initialize_user_system(self):
        """Initialize user and permissions system with encrypted storage and HMAC"""
        # Create users file if it doesn't exist
        if not os.path.exists(self.users_file):
            # Create admin user by default with encrypted storage
            # Hash the password with bcrypt
            bcrypt_hash = self._hash_password("admin")
            # Apply HMAC to the bcrypt hash for additional protection
            hmac_value = self._hmac_password(bcrypt_hash)
            
            users_data = {
                "admin": {
                    "password_hash": bcrypt_hash,
                    "password_hmac": hmac_value,
                    "is_admin": True,
                    "created_at": datetime.now().isoformat()
                }
            }
            
            # Save encrypted user data
            self._save_encrypted_json(self.users_file, users_data)
            log_event("Created new users file with default admin account and HMAC protection")
                
        # Create permissions file if it doesn't exist
        if not os.path.exists(self.permissions_file):
            # Admin has all permissions
            permissions_data = {
                "admin": {
                    "*": ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "GRANT", "REVOKE"],
                    "created_at": datetime.now().isoformat()
                }
            }
            
            # Save encrypted permissions data
            self._save_encrypted_json(self.permissions_file, permissions_data)
            log_event("Created new permissions file with admin privileges")

    def _hash_password(self, password):
        """Hash a password using bcrypt with salt for secure storage"""
        # Convert password to bytes if it's a string
        if isinstance(password, str):
            password = password.encode('utf-8')
        
        # Generate a salt and hash the password
        salt = bcrypt.gensalt(rounds=12)  # Higher rounds = more secure, but slower
        hashed = bcrypt.hashpw(password, salt)
        
        # Return the hash as a string for storage
        return hashed.decode('utf-8')
        
    def _authenticate_user(self, username, password):
        """Authenticate a user by username and password using bcrypt + HMAC"""
        try:
            # Load encrypted user data
            users = self._load_encrypted_json(self.users_file)
                
            if username in users:
                user_data = users[username]
                stored_hash = user_data["password_hash"]
                
                # Check if we need to migrate to the new HMAC system
                if "password_hmac" not in user_data:
                    # Convert input password to bytes
                    if isinstance(password, str):
                        password = password.encode('utf-8')
                    
                    # Convert stored hash to bytes if it's a string
                    if isinstance(stored_hash, str):
                        stored_hash = stored_hash.encode('utf-8')
                    
                    # Legacy check with just bcrypt
                    if bcrypt.checkpw(password, stored_hash):
                        # Migrate to the new system
                        hmac_value = self._hmac_password(stored_hash)
                        users[username]["password_hmac"] = hmac_value
                        self._save_encrypted_json(self.users_file, users)
                        log_event(f"Migrated user '{username}' to HMAC authentication")
                        return True
                else:
                    # Use the new HMAC verification system
                    if self._verify_hmac_password(password, user_data["password_hmac"], stored_hash):
                        log_event(f"User '{username}' authenticated successfully")
                        return True
                    else:
                        log_event(f"Failed authentication attempt for user '{username}'", "WARNING")
            else:
                log_event(f"Authentication attempt for non-existent user '{username}'", "WARNING")
            
            return False
        except Exception as e:
            log_event(f"Authentication error: {e}", "ERROR")
            return False
            
    def _check_permission(self, username, operation, table_name):
        """Check if a user has permission to perform an operation on a table"""
        try:
            # Load encrypted permissions and user data
            permissions = self._load_encrypted_json(self.permissions_file)
            users = self._load_encrypted_json(self.users_file)
                
            # Admin has all permissions
            if username in users and users[username].get("is_admin", False):
                return True
                
            # Check user permissions
            if username in permissions:
                user_perms = permissions[username]
                
                # Check for wildcard table permissions
                if "*" in user_perms and operation in user_perms["*"]:
                    return True
                    
                # Check for specific table permissions
                if table_name in user_perms and operation in user_perms[table_name]:
                    return True
            
            return False
        except Exception as e:
            log_event(f"Permission check error: {e}", "ERROR")
            return False
            
    def _create_user(self, username, password):
        """Create a new user with encrypted storage and HMAC"""
        try:
            # Load encrypted user data
            users = self._load_encrypted_json(self.users_file)
                
            if username in users:
                return {"error": f"User '{username}' already exists"}
                
            # Hash the password with bcrypt
            bcrypt_hash = self._hash_password(password)
            # Apply HMAC to the bcrypt hash for additional protection
            hmac_value = self._hmac_password(bcrypt_hash)
            
            users[username] = {
                "password_hash": bcrypt_hash,
                "password_hmac": hmac_value,
                "is_admin": False,
                "created_at": datetime.now().isoformat(),
                "created_by": self.current_user
            }
            
            # Save encrypted user data
            self._save_encrypted_json(self.users_file, users)
                
            # Initialize empty permissions
            permissions = self._load_encrypted_json(self.permissions_file)
            
            permissions[username] = {
                "created_at": datetime.now().isoformat()
            }
            
            # Save encrypted permissions data
            self._save_encrypted_json(self.permissions_file, permissions)
                
            log_event(f"User '{username}' created by '{self.current_user}'")
            return {"message": f"User '{username}' created successfully"}
        except Exception as e:
            log_event(f"User creation error: {e}", "ERROR")
            return {"error": f"Failed to create user: {e}"}
            
    def _grant_permission(self, permission, table_name, username):
        """Grant a permission to a user"""
        try:
            # Verify the user exists
            users = self._load_encrypted_json(self.users_file)
                
            if username not in users:
                return {"error": f"User '{username}' does not exist"}
                
            # Grant permission
            permissions = self._load_encrypted_json(self.permissions_file)
                
            if username not in permissions:
                permissions[username] = {}
                
            if table_name not in permissions[username]:
                permissions[username][table_name] = []
                
            if permission not in permissions[username][table_name]:
                permissions[username][table_name].append(permission)
                
            # Save encrypted permissions data
            self._save_encrypted_json(self.permissions_file, permissions)
                
            log_event(f"Permission '{permission}' on '{table_name}' granted to '{username}' by '{self.current_user}'")
            return {"message": f"Permission '{permission}' on '{table_name}' granted to '{username}'"}
        except Exception as e:
            log_event(f"Grant permission error: {e}", "ERROR")
            return {"error": f"Failed to grant permission: {e}"}
            
    def _revoke_permission(self, permission, table_name, username):
        """Revoke a permission from a user"""
        try:
            # Verify the user exists
            users = self._load_encrypted_json(self.users_file)
                
            if username not in users:
                return {"error": f"User '{username}' does not exist"}
                
            # Revoke permission
            permissions = self._load_encrypted_json(self.permissions_file)
                
            if username in permissions and table_name in permissions[username] and permission in permissions[username][table_name]:
                permissions[username][table_name].remove(permission)
                
                # Save encrypted permissions data
                self._save_encrypted_json(self.permissions_file, permissions)
                    
                log_event(f"Permission '{permission}' on '{table_name}' revoked from '{username}' by '{self.current_user}'")
                return {"message": f"Permission '{permission}' on '{table_name}' revoked from '{username}'"}
            else:
                return {"error": f"User '{username}' does not have '{permission}' permission on '{table_name}'"}
        except Exception as e:
            log_event(f"Revoke permission error: {e}", "ERROR")
            return {"error": f"Failed to revoke permission: {e}"}

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
        
        # Authenticate user before allowing access
        self._handle_login()
        
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
                tx_prompt = self._colorize(f"SQL ({self.current_user}/TX:{tx_id} | Ops:{tx_ops})> ", "1;33")  # Bold yellow
            else:
                tx_prompt = f"SQL ({self.current_user})> "
                
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
            elif query.lower() == "logout":
                self._handle_logout()
                return
                
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

            # Handle user management and permission commands
            try:
                # Basic parsing for special commands
                if query.upper().startswith("CREATE USER"):
                    parts = query.split()
                    if len(parts) >= 5 and parts[2] == "IDENTIFIED" and parts[3] == "BY":
                        username = parts[1]
                        # Extract password (might be quoted)
                        password_start = query.find("BY") + 3
                        password = query[password_start:].strip()
                        if password.startswith("'") and password.endswith("'"):
                            password = password[1:-1]
                        elif password.startswith('"') and password.endswith('"'):
                            password = password[1:-1]
                            
                        # Only admin can create users
                        with open(self.users_file, 'r') as f:
                            users = json.load(f)
                            if self.current_user in users and users[self.current_user].get("is_admin", False):
                                result = self._create_user(username, password)
                                if "error" in result:
                                    print(self._colorize(f"❌ Error: {result['error']}", "1;31"))
                                else:
                                    print(self._colorize("✅ " + result.get("message", "User created."), "1;32"))
                            else:
                                print(self._colorize(f"❌ Error: Only administrators can create users", "1;31"))
                                log_event(f"User '{self.current_user}' attempted to create user without admin rights")
                    else:
                        print(self._colorize("❌ Error: Invalid CREATE USER syntax. Use: CREATE USER username IDENTIFIED BY 'password'", "1;31"))
                    continue
                    
                elif query.upper().startswith("GRANT"):
                    parts = query.upper().split()
                    if len(parts) >= 5 and "ON" in parts and "TO" in parts:
                        # Extract grant details
                        permission = parts[1]  # e.g., SELECT
                        on_index = parts.index("ON")
                        to_index = parts.index("TO")
                        
                        if on_index + 1 < to_index and to_index + 1 < len(parts):
                            table_name = parts[on_index + 1]
                            username = parts[to_index + 1]
                            
                            # Only admin can grant permissions
                            with open(self.users_file, 'r') as f:
                                users = json.load(f)
                                if self.current_user in users and users[self.current_user].get("is_admin", False):
                                    result = self._grant_permission(permission, table_name, username)
                                    if "error" in result:
                                        print(self._colorize(f"❌ Error: {result['error']}", "1;31"))
                                    else:
                                        print(self._colorize("✅ " + result.get("message", "Permission granted."), "1;32"))
                                else:
                                    print(self._colorize(f"❌ Error: Only administrators can grant permissions", "1;31"))
                                    log_event(f"User '{self.current_user}' attempted to grant permissions without admin rights")
                        else:
                            print(self._colorize("❌ Error: Invalid GRANT syntax. Use: GRANT permission ON table TO username", "1;31"))
                    else:
                        print(self._colorize("❌ Error: Invalid GRANT syntax. Use: GRANT permission ON table TO username", "1;31"))
                    continue
                    
                elif query.upper().startswith("REVOKE"):
                    parts = query.upper().split()
                    if len(parts) >= 5 and "ON" in parts and "FROM" in parts:
                        # Extract revoke details
                        permission = parts[1]  # e.g., SELECT
                        on_index = parts.index("ON")
                        from_index = parts.index("FROM")
                        
                        if on_index + 1 < from_index and from_index + 1 < len(parts):
                            table_name = parts[on_index + 1]
                            username = parts[from_index + 1]
                            
                            # Only admin can revoke permissions
                            with open(self.users_file, 'r') as f:
                                users = json.load(f)
                                if self.current_user in users and users[self.current_user].get("is_admin", False):
                                    result = self._revoke_permission(permission, table_name, username)
                                    if "error" in result:
                                        print(self._colorize(f"❌ Error: {result['error']}", "1;31"))
                                    else:
                                        print(self._colorize("✅ " + result.get("message", "Permission revoked."), "1;32"))
                                else:
                                    print(self._colorize(f"❌ Error: Only administrators can revoke permissions", "1;31"))
                                    log_event(f"User '{self.current_user}' attempted to revoke permissions without admin rights")
                        else:
                            print(self._colorize("❌ Error: Invalid REVOKE syntax. Use: REVOKE permission ON table FROM username", "1;31"))
                    else:
                        print(self._colorize("❌ Error: Invalid REVOKE syntax. Use: REVOKE permission ON table FROM username", "1;31"))
                    continue
                    
                # For standard SQL queries, parse and check permissions
                parsed_query = self.parser.parse(query)
                command = parsed_query["command"].upper()
                
                # Extract table name for permission checking
                table_name = None
                if "table_name" in parsed_query:
                    table_name = parsed_query["table_name"]
                elif "tokens" in parsed_query:
                    # Try to extract table name from tokens (simple approach)
                    tokens = parsed_query["tokens"]
                    for i, token in enumerate(tokens):
                        if token.upper() in ["FROM", "INTO", "UPDATE", "TABLE"] and i+1 < len(tokens):
                            table_name = tokens[i+1]
                            break
                
                # Check permissions
                if table_name and command not in ["BEGIN", "COMMIT", "ROLLBACK"]:
                    if not self._check_permission(self.current_user, command, table_name):
                        print(self._colorize(f"❌ Permission denied: You don't have {command} permission on {table_name}", "1;31"))
                        log_event(f"Permission denied: User '{self.current_user}' attempted {command} on {table_name}")
                        continue
                
                # Show transaction status if inside transaction
                if self.transaction_active and command in ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP"]:
                    print(self._colorize(f"📝 Operation '{command}' will be added to current transaction", "1;36"))
                
                # Log the command execution
                log_event(f"User '{self.current_user}' executed: {command}")
                
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
                log_event(f"Error executing query: {e}", "ERROR")
                
    def _handle_login(self):
        """Handle user login"""
        print(self._colorize("=== Authentication Required ===", "1;36"))
        max_attempts = 3
        attempt = 0
        
        while attempt < max_attempts:
            username = input("Username: ")
            password = input("Password: ")  # In a real app, use getpass
            
            if self._authenticate_user(username, password):
                self.authenticated = True
                self.current_user = username
                print(self._colorize(f"Logged in as: {username}", "1;32"))
                return
            else:
                attempt += 1
                remaining = max_attempts - attempt
                if remaining > 0:
                    print(self._colorize(f"Authentication failed. {remaining} attempts remaining.", "1;31"))
                else:
                    print(self._colorize("Authentication failed. Exiting.", "1;31"))
                    self.exit_requested = True
                    sys.exit(1)
                    
    def _handle_logout(self):
        """Handle user logout"""
        self.authenticated = False
        self.current_user = None
        print(self._colorize("Logged out successfully", "1;32"))
        print("Please login again to continue.")
        self._handle_login()
                
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
        print(self._colorize("\nUser Management:", "1;33"))
        print("  CREATE USER username IDENTIFIED BY 'password' - Create a new user")
        print("  GRANT permission ON table TO username - Grant permission to user")
        print("  REVOKE permission ON table FROM username - Revoke permission from user")
        print(self._colorize("\nOther Commands:", "1;33"))
        print("  exit - Exit the SQL shell")
        print("  help - Display this help information")
        print("  status - Show current transaction details")
        print("  logout - Log out the current user")
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
        
        # Admin reset command
        if command == 'admin_reset' or command == '\\ar':
            self._handle_admin_reset()
            return
            
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
    
    def _handle_admin_reset(self):
        """Handle admin reset command - creates new admin account if users.json is corrupted or lost"""
        print(self._colorize("\n⚠️  WARNING: ADMIN RESET REQUESTED ⚠️", "1;31;47"))
        print(self._colorize("This will create a new admin account with default password!", "1;31"))
        print("This should only be done if the admin account is locked out or users.json is corrupted.")
        confirmation = input("Type 'CONFIRM RESET' to proceed: ")
        
        if confirmation != "CONFIRM RESET":
            print("Admin reset cancelled.")
            return
            
        try:
            # Create a new admin account with default password
            # Hash the password with bcrypt
            bcrypt_hash = self._hash_password("admin")
            # Apply HMAC to the bcrypt hash for additional protection
            hmac_value = self._hmac_password(bcrypt_hash)
            
            users_data = {
                "admin": {
                    "password_hash": bcrypt_hash,
                    "password_hmac": hmac_value,
                    "is_admin": True,
                    "created_at": datetime.now().isoformat(),
                    "reset_by": "admin_reset_command"
                }
            }
            
            # Save encrypted user data
            self._save_encrypted_json(self.users_file, users_data)
            
            # Create default permissions for admin
            permissions_data = {
                "admin": {
                    "*": ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "GRANT", "REVOKE"],
                    "created_at": datetime.now().isoformat()
                }
            }
            
            # Save encrypted permissions data
            self._save_encrypted_json(self.permissions_file, permissions_data)
            
            print(self._colorize("✅ Admin account reset successfully!", "1;32"))
            print("Username: admin")
            print("Password: admin")
            print(self._colorize("⚠️ IMPORTANT: Change this password immediately after login!", "1;31"))
            
            # Log the reset
            log_event("ADMIN ACCOUNT RESET performed - new admin account created", "WARNING")
            
        except Exception as e:
            print(self._colorize(f"❌ Error resetting admin account: {e}", "1;31"))
            log_event(f"Admin reset attempt failed: {e}", "ERROR")

    def _show_help(self):
        """Show help information"""
        help_msg = """
Available commands:
  help, \\h           Show this help message
  exit, \\q           Exit the CLI
  status, \\s         Show transaction status
  debug, \\d          Toggle transaction debug mode
  logout             Log out the current user
  admin_reset, \\ar   Reset admin account (emergency use only)
  
Transaction Commands:
  begin               Start a new transaction
  commit              Commit the current transaction
  rollback            Rollback the current transaction

User Management:
  CREATE USER username IDENTIFIED BY 'password'
  GRANT permission ON table TO username
  REVOKE permission ON table FROM username

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

