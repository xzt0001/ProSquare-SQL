import hashlib
import json
import os
from utils.logger import log_event

# User database file
USER_DB = "data/users.json"

# Ensure user database exists
def initialize_user_db():
    if not os.path.exists(USER_DB):
        with open(USER_DB, "w") as f:
            json.dump({}, f)

def hash_password(password: str) -> str:
    """Hashes a password using SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()

def create_user(username: str, password: str, role: str = "user"):
    """Creates a new user with a hashed password and assigned role."""
    initialize_user_db()
    
    with open(USER_DB, "r") as f:
        users = json.load(f)
    
    if username in users:
        raise ValueError("User already exists.")
    
    users[username] = {
        "password": hash_password(password),
        "role": role,
        "permissions": []  # Empty by default
    }
    
    with open(USER_DB, "w") as f:
        json.dump(users, f, indent=4)
    
    log_event(f"User '{username}' created with role '{role}'.")

def authenticate_user(username: str, password: str) -> bool:
    """Validates username and password."""
    initialize_user_db()
    
    with open(USER_DB, "r") as f:
        users = json.load(f)
    
    if username not in users:
        log_event(f"Failed login attempt for '{username}'.")
        return False
    
    if users[username]["password"] == hash_password(password):
        return True
    
    log_event(f"Failed login attempt for '{username}'.")
    return False

def grant_permission(username: str, permission: str):
    """Grants a specific permission to a user."""
    initialize_user_db()
    
    with open(USER_DB, "r") as f:
        users = json.load(f)
    
    if username not in users:
        raise ValueError("User does not exist.")
    
    if permission not in users[username]["permissions"]:
        users[username]["permissions"].append(permission)
    
    with open(USER_DB, "w") as f:
        json.dump(users, f, indent=4)
    
    log_event(f"Granted {permission} to '{username}'.")

def revoke_permission(username: str, permission: str):
    """Revokes a specific permission from a user."""
    initialize_user_db()
    
    with open(USER_DB, "r") as f:
        users = json.load(f)
    
    if username not in users:
        raise ValueError("User does not exist.")
    
    if permission in users[username]["permissions"]:
        users[username]["permissions"].remove(permission)
    
    with open(USER_DB, "w") as f:
        json.dump(users, f, indent=4)
    
    log_event(f"Revoked {permission} from '{username}'.")

def check_permission(username: str, query_type: str) -> bool:
    """Checks if a user has permission for a given query type."""
    initialize_user_db()
    
    with open(USER_DB, "r") as f:
        users = json.load(f)
    
    if username not in users:
        return False
    
    return query_type in users[username]["permissions"]
