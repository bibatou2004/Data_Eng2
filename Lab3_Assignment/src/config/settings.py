"""
Configuration et paramètres pour Lab 3 Assignment
"""

import os
from pathlib import Path

# Chemins
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "input"
OUTPUT_PATH = PROJECT_ROOT / "data" / "output"

# Spark Configuration
SPARK_CONFIG = {
    "app_name": "Lab3-Assignment",
    "master": "local[*]",
    "driver_memory": "8g",
    "executor_memory": "8g",
    "shuffle_partitions": 400,
    "adaptive_enabled": True,
}

# Données
TABLES = {
    "events": "retail_dw_20250826_events",
    "products": "retail_dw_20250826_products",
    "brands": "retail_dw_20250826_brands",
}

# Performance monitoring
MONITOR_MEMORY = True
MONITOR_TIME = True

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"

