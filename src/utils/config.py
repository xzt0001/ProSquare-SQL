# utils/config.py
import os

# Default database settings
DATABASE_PATH = os.path.join(os.getcwd(), "data")

# Logging settings
LOG_FILE = os.path.join(DATABASE_PATH, "database.log")

# Ensure data directory exists
if not os.path.exists(DATABASE_PATH):
    os.makedirs(DATABASE_PATH)
