"""
Spark utilities for session management and optimization.
Contains functions for creating and configuring Spark sessions.
"""

import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from config.spark_config import SPARK_CONFIG, APP_SPARK_CONFIG

logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create and configure a Spark session with optimized settings.
    
    Args:
        app_name: Application name for Spark
        master: Master URL for Spark cluster
        config: Additional configuration parameters
        
    Returns:
        Configured SparkSession
    """
    app_name = app_name or APP_SPARK_CONFIG["app_name"]
    master = master or APP_SPARK_CONFIG["master"]
    
    # Start building Spark session
    builder = SparkSession.builder.appName(app_name).master(master)
    
    # Apply default Spark configuration
    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)
    
    # Apply custom configuration if provided
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
    
    # Create Spark session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel(APP_SPARK_CONFIG["log_level"])
    
    logger.info(f"Created Spark session: {app_name}")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Master URL: {master}")
    
    return spark


def optimize_spark_session(spark: SparkSession) -> SparkSession:
    """
    Apply additional optimizations to an existing Spark session.
    
    Args:
        spark: Existing SparkSession
        
    Returns:
        Optimized SparkSession
    """
    # Enable adaptive query execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Optimize broadcast joins
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
    
    # Optimize shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    
    logger.info("Applied additional Spark optimizations")
    return spark


def get_spark_metrics(spark: SparkSession) -> Dict[str, Any]:
    """
    Get current Spark session metrics.
    
    Args:
        spark: SparkSession
        
    Returns:
        Dictionary containing Spark metrics
    """
    metrics = {
        "spark_version": spark.version,
        "master_url": spark.conf.get("spark.master"),
        "app_name": spark.conf.get("spark.app.name"),
        "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "broadcast_threshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    }
    
    return metrics


def stop_spark_session(spark: SparkSession) -> None:
    """
    Safely stop a Spark session.
    
    Args:
        spark: SparkSession to stop
    """
    try:
        spark.stop()
        logger.info("Spark session stopped successfully")
    except Exception as e:
        logger.error(f"Error stopping Spark session: {e}")


def validate_spark_session(spark: SparkSession) -> bool:
    """
    Validate that a Spark session is properly configured.
    
    Args:
        spark: SparkSession to validate
        
    Returns:
        True if session is valid, False otherwise
    """
    try:
        # Test basic functionality
        test_df = spark.range(1)
        test_df.collect()
        
        # Check required configurations
        required_configs = [
            "spark.sql.adaptive.enabled",
            "spark.sql.adaptive.coalescePartitions.enabled",
            "spark.sql.autoBroadcastJoinThreshold"
        ]
        
        for config in required_configs:
            if not spark.conf.get(config):
                logger.warning(f"Required configuration missing: {config}")
                return False
        
        logger.info("Spark session validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Spark session validation failed: {e}")
        return False
