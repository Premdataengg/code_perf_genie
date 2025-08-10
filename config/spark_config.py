"""
Spark configuration settings for optimal performance.
Contains all Spark-related configuration parameters.
"""

# Spark session configuration for optimal performance
SPARK_CONFIG = {
    # Adaptive Query Execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
    "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
    
    # Memory and execution optimization
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
    "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256m",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    
    # Broadcast join optimization
    "spark.sql.autoBroadcastJoinThreshold": "10m",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "10m",
    
    # Partition management
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "200",
    
    # Shuffle optimization
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    
    # Memory management
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
    "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
    
    # Performance monitoring
    "spark.sql.statistics.size.autoUpdate.enabled": "true",
    "spark.sql.statistics.histogram.enabled": "true",
    "spark.sql.statistics.histogram.numBins": "254",
    
    # Logging and debugging
    "spark.sql.adaptive.logLevel": "INFO",
    "spark.sql.adaptive.forceApply": "false"
}

# Application-specific Spark settings
APP_SPARK_CONFIG = {
    "app_name": "PySpark SQL Performance Demo",
    "master": "local[*]",
    "log_level": "WARN"
}

# Performance testing configuration
PERFORMANCE_CONFIG = {
    "enable_monitoring": True,
    "collect_metrics": True,
    "log_execution_plans": True,
    "track_memory_usage": True,
    "track_cpu_usage": True
}

# Dataset configuration
DATASET_CONFIG = {
    "employees_count": 30,
    "departments_count": 7,
    "projects_count": 10,
    "large_dataset_size": 1000,
    "partition_count": 4
}
