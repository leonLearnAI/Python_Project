"""
Configuration settings for Traffic Safety Analysis
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parent
SCHEMAS_DIR = BASE_DIR / "schemas"
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
LOGS_DIR = BASE_DIR / "logs"

# Ensure directories exist
for directory in [SCHEMAS_DIR, DATA_DIR, REPORTS_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True)

# MongoDB configuration
MONGODB_CONFIG = {
    "uri": os.getenv("MONGODB_URI", "mongodb://localhost:27017/"),
    "database": os.getenv("MONGODB_DB", "traffic_safety_db"),
    "timeout": 5000,
    "max_pool_size": 50,
    "retry_writes": True,
    "compressors": "zlib"
}

# Collection names
COLLECTIONS = {
    "crash_reports": "crash_reports",
    "traffic_flow": "traffic_flow", 
    "incidents": "incidents",
    "analytics": "analytics_results"
}

# Schema files
SCHEMA_FILES = {
    "crash_reports": SCHEMAS_DIR / "crash_reporting_schema.json",
    "traffic_flow": SCHEMAS_DIR / "traffic_flow_schema.json",
    "incidents": SCHEMAS_DIR / "incidents_schema.json",
    "analytics": SCHEMAS_DIR / "analytics_schema.json"
}

# Data files
DATA_FILES = {
    "crash_reports": DATA_DIR / "Crash_Reporting_-_Drivers_Data.csv",
    "traffic_flow": DATA_DIR / "Traffic_Flow_Data_Jan_to_June_2023_SDCC.csv",
    "incidents": DATA_DIR / "cpd-crash-incidents.json"
}

# Processing settings
PROCESSING_CONFIG = {
    "batch_size": 1000,
    "sample_size": None,  # Set to number for testing, None for full dataset
    "enable_validation": True,
    "enable_indexing": True,
    "max_workers": 4,
    "chunk_size": 10000
}

# Analysis settings
ANALYSIS_CONFIG = {
    "enable_charts": True,
    "chart_format": "png",
    "chart_dpi": 150,
    "save_analytics": True,
    "generate_report": True,
    "report_format": "html",
    "analysis_types": [
        "temporal_analysis",
        "weather_impact_analysis", 
        "vehicle_analysis",
        "spatial_analysis",
        "correlation_analysis"
    ]
}

# Visualization settings
VISUALIZATION_CONFIG = {
    "color_palette": "Set2",
    "figure_size": (16, 10),
    "font_size": 12,
    "save_path": REPORTS_DIR / "charts",
    "formats": ["png", "pdf"]
}

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": LOGS_DIR / "traffic_analysis.log",
    "max_bytes": 10485760,  # 10MB
    "backup_count": 5
}

# Geospatial settings
GEO_CONFIG = {
    "default_crs": "EPSG:4326",  # WGS84
    "buffer_distance_km": 0.5,
    "grid_size_degrees": 0.01,
    "hotspot_threshold": 10
}

# Data quality thresholds
QUALITY_THRESHOLDS = {
    "min_coordinates": 0.7,  # Minimum % of records with coordinates
    "max_missing_values": 0.3,  # Maximum % missing values allowed
    "min_date_range_days": 30,  # Minimum date range in days
    "data_completeness_score": 0.8  # Minimum data completeness score
}

# API and external services
EXTERNAL_SERVICES = {
    "weather_api": {
        "enabled": False,
        "api_key": os.getenv("WEATHER_API_KEY", ""),
        "endpoint": "https://api.weather.com/v3"
    },
    "map_service": {
        "enabled": True,
        "provider": "openstreetmap",
        "tile_url": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    }
}

# Performance settings
PERFORMANCE_CONFIG = {
    "use_cache": True,
    "cache_ttl": 3600,  # 1 hour
    "query_timeout": 30000,  # 30 seconds
    "max_documents_per_query": 100000
}
