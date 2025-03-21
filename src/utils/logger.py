import os
import datetime
import hashlib
import logging
import keyring
import secrets
import random
from logging.handlers import RotatingFileHandler

# Define log files and directories
LOG_DIR = "logs"
SECURITY_LOG = os.path.join(LOG_DIR, "security.log")
HASH_FILE = os.path.join(LOG_DIR, "security.log.hash")
KEYRING_SERVICE = "ProSquareSQL"
KEYRING_LOG_HASH = "security_log_hash"
KEYRING_LOG_TIMESTAMP = "security_log_timestamp"

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

class SecureRotatingFileHandler(RotatingFileHandler):
    """A rotating file handler that creates tamper-evident logs through hash chaining"""
    
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        # Force append mode for security
        if 'a' not in mode:
            mode = 'a'
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self.prev_hash = None
        self.hash_file = f"{filename}.hash"
        self.append_only = True  # Flag to indicate we're enforcing append-only mode
        
        # Initialize or load the previous hash
        if os.path.exists(self.hash_file):
            try:
                with open(self.hash_file, 'r') as f:
                    self.prev_hash = f.read().strip()
            except Exception:
                # If there's any error, start fresh
                self.prev_hash = self._compute_file_hash(filename)
                with open(self.hash_file, 'w') as f:
                    f.write(self.prev_hash)
        else:
            # Initialize with current file hash if it exists
            if os.path.exists(filename):
                self.prev_hash = self._compute_file_hash(filename)
            else:
                self.prev_hash = hashlib.sha256(b"").hexdigest()
            
            with open(self.hash_file, 'w') as f:
                f.write(self.prev_hash)
                
        # Verify integrity when handler is initialized
        self._verify_integrity_on_startup()
    
    def _verify_integrity_on_startup(self):
        """Verify log integrity when the handler starts up"""
        if os.path.exists(self.baseFilename) and os.path.getsize(self.baseFilename) > 0:
            result, message = verify_log_integrity()
            if not result:
                # Log the integrity failure but continue (don't block logging)
                # We'll use a direct file write since logger might not be set up yet
                error_msg = f"CRITICAL: {datetime.datetime.now().isoformat()} - Log integrity check failed on startup: {message}"
                try:
                    with open(self.baseFilename, 'a') as f:
                        f.write(f"\n{error_msg}\n")
                except Exception:
                    pass  # Can't do much if we can't write to the log
    
    def _compute_file_hash(self, filename):
        """Compute SHA-256 hash of a file"""
        if not os.path.exists(filename):
            return hashlib.sha256(b"").hexdigest()
            
        sha256 = hashlib.sha256()
        with open(filename, 'rb') as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256.update(block)
        return sha256.hexdigest()
    
    def _open(self):
        """Override _open to use append-only mode for additional security"""
        if getattr(self, 'baseFilename', None):
            # Use O_APPEND flag for true append-only operation
            try:
                import fcntl
                # Get file descriptor for the log file
                fd = os.open(self.baseFilename, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
                # Return a file object with the descriptor
                return os.fdopen(fd, 'a')
            except (ImportError, AttributeError):
                # Fallback to normal behavior if fcntl is not available
                return open(self.baseFilename, 'a')
        return super()._open()
        
    def emit(self, record):
        """Override emit to add hash chaining"""
        # Get the message content
        msg = self.format(record)
        
        # Create a hash of the previous hash + current message
        current_hash = hashlib.sha256(f"{self.prev_hash}{msg}".encode()).hexdigest()
        
        # Add the hash to the record
        record.hash = current_hash
        
        # Call the parent emit method
        super().emit(record)
        
        # Update the previous hash
        self.prev_hash = current_hash
        with open(self.hash_file, 'w') as f:
            f.write(current_hash)
            
        # Also store the hash in the system keyring for additional security
        try:
            keyring.set_password(KEYRING_SERVICE, KEYRING_LOG_HASH, current_hash)
            keyring.set_password(KEYRING_SERVICE, KEYRING_LOG_TIMESTAMP, datetime.datetime.now().isoformat())
        except Exception:
            # If keyring fails, continue without it
            pass

# Set up the secure logger
security_logger = logging.getLogger("security")
security_logger.setLevel(logging.INFO)

# Create handler with 10MB max file size and 5 backup files
handler = SecureRotatingFileHandler(
    SECURITY_LOG,
    maxBytes=10*1024*1024,
    backupCount=5
)

# Format the log entries
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# Add handler to logger
security_logger.addHandler(handler)

# Track last integrity check to avoid overhead
_last_integrity_check = datetime.datetime.now()
_integrity_check_interval = datetime.timedelta(minutes=5)  # Check every 5 minutes
_integrity_check_random = 10  # 1/10 chance of checking outside interval

def log_event(message: str, level="INFO"):
    """Logs an event with tamper-proof hash chaining and periodic integrity verification"""
    global _last_integrity_check, _integrity_check_interval
    
    # Periodically verify log integrity to detect tampering
    now = datetime.datetime.now()
    # Check if it's time for an integrity check or if we hit the random check
    if (now - _last_integrity_check > _integrity_check_interval or 
            random.randint(1, _integrity_check_random) == 1):
        result, msg = verify_log_integrity()
        if not result:
            # Also log the integrity failure
            security_logger.critical(f"LOG INTEGRITY CHECK FAILED: {msg}")
        _last_integrity_check = now
    
    # Log the original message
    if level.upper() == "ERROR":
        security_logger.error(message)
    elif level.upper() == "WARNING":
        security_logger.warning(message)
    else:
        security_logger.info(message)

def verify_log_integrity():
    """Verifies the integrity of the log file by recalculating and comparing hashes"""
    # First check if log file exists
    if not os.path.exists(SECURITY_LOG):
        # Check if we have a keyring hash but no file
        keyring_hash = None
        try:
            keyring_hash = keyring.get_password(KEYRING_SERVICE, KEYRING_LOG_HASH)
            timestamp = keyring.get_password(KEYRING_SERVICE, KEYRING_LOG_TIMESTAMP)
            if keyring_hash:
                return False, f"Log file is missing but should exist! Last hash timestamp: {timestamp}"
        except Exception:
            pass
        return False, "Log file does not exist"
        
    # Get the expected final hash from file
    file_hash = None
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, 'r') as f:
            file_hash = f.read().strip()
    
    # Get the expected final hash from keyring for additional verification
    keyring_hash = None
    try:
        keyring_hash = keyring.get_password(KEYRING_SERVICE, KEYRING_LOG_HASH)
    except Exception:
        pass
    
    # If we don't have any hash to verify against
    if not file_hash and not keyring_hash:
        return False, "No hash records found to verify against"
        
    # Compute current hash from log file
    current_hash = hashlib.sha256(b"").hexdigest()
    
    # Re-compute hash chain
    with open(SECURITY_LOG, 'r') as f:
        for line in f:
            # Skip empty lines
            if not line.strip():
                continue
            current_hash = hashlib.sha256(f"{current_hash}{line.strip()}".encode()).hexdigest()
    
    # Check against file hash if available
    if file_hash and current_hash == file_hash:
        # Also check against keyring hash if available
        if keyring_hash and keyring_hash != file_hash:
            return False, "Keyring hash doesn't match file hash - possible hash file tampering"
        return True, "Log integrity verified"
            
    # If file hash failed but keyring hash is available
    if keyring_hash:
        if current_hash == keyring_hash:
            # Hash file is wrong but keyring is correct - update hash file
            with open(HASH_FILE, 'w') as f:
                f.write(current_hash)
            return True, "Log integrity verified (hash file updated from keyring)"
        else:
            return False, "Log integrity check failed - logs tampered with (both file and keyring hashes mismatch)"
    
    # If we only had file hash and it failed
    return False, "Log integrity check failed: logs may have been tampered with"
