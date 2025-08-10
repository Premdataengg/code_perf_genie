"""
Application configuration settings.
Contains application-specific configuration parameters.
"""

# Application configuration
APP_CONFIG = {
    "name": "PySpark SQL Performance Demo",
    "version": "1.0.0",
    "description": "Demonstrates Spark SQL performance optimization techniques",
    "author": "Performance Demo Team",
    "log_level": "INFO",
    "output_format": "table",
    "enable_colors": True,
    "max_display_rows": 20,
    "truncate_strings": True
}

# Query execution configuration
QUERY_CONFIG = {
    "timeout_seconds": 300,
    "max_retries": 3,
    "enable_validation": True,
    "show_execution_plans": False,
    "collect_metrics": True
}

# Output formatting configuration
OUTPUT_CONFIG = {
    "table_format": "grid",
    "max_column_width": 50,
    "show_row_numbers": True,
    "truncate_long_strings": True,
    "enable_colors": True
}

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "logs/app.log",
    "max_file_size": "10MB",
    "backup_count": 5
}

# Performance monitoring configuration
MONITORING_CONFIG = {
    "enable_monitoring": True,
    "track_execution_time": True,
    "track_memory_usage": True,
    "track_cpu_usage": True,
    "track_shuffle_operations": True,
    "track_io_operations": True,
    "generate_reports": True,
    "report_format": "json"
}

# Development configuration
DEV_CONFIG = {
    "debug_mode": False,
    "verbose_output": False,
    "show_sql_queries": True,
    "show_dataframe_schemas": False,
    "enable_profiling": False
}

# Dataset configuration
DATASET_CONFIG = {
    "employees_count": 30,
    "departments_count": 7,
    "projects_count": 10,
    "large_dataset_size": 1000,
    "partition_count": 4
}
