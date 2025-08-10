#!/usr/bin/env python3
"""
PySpark SQL Performance Demo - Main Application

This script demonstrates Spark SQL performance optimization techniques
by running both anti-pattern queries (what NOT to do) and best practice
queries (what TO do) for optimal Spark performance.
"""

import argparse
import logging
import sys
from typing import Optional

# Add src to path for imports
sys.path.append('src')

from src.common.spark_utils import create_spark_session, stop_spark_session, validate_spark_session
from src.common.data_generator import create_sample_dataframes, register_temp_views
from src.common.query_loader import (
    get_sample_queries, 
    format_query_result, 
    print_separator, 
    list_available_queries,
    get_query_statistics
)
from config.app_config import APP_CONFIG, QUERY_CONFIG


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/app.log', mode='w')
        ]
    )


def run_queries(spark, query_type: str = "all") -> None:
    """
    Run SQL queries based on the specified type.
    
    Args:
        spark: SparkSession
        query_type: Type of queries to run ("all", "anti_patterns", "best_practices")
    """
    # Get queries
    queries = get_sample_queries(query_type)
    
    if not queries:
        print(f"No queries found for type: {query_type}")
        return
    
    # Display query statistics
    stats = get_query_statistics(queries)
    print_separator(f"RUNNING {query_type.upper().replace('_', ' ')} QUERIES")
    print(f"Total queries: {stats['total_queries']}")
    print(f"Anti-patterns: {stats['anti_patterns']}")
    print(f"Best practices: {stats['best_practices']}")
    
    # Run each query
    for i, query_info in enumerate(queries, 1):
        print_separator(f"QUERY {i}: {query_info['title']}")
        print(f"Type: {query_info['type'].replace('_', ' ').title()}")
        print(f"Description: {query_info['description']}")
        print(f"File: {query_info['file']}")
        
        try:
            # Validate query if enabled
            if QUERY_CONFIG["enable_validation"]:
                from src.common.query_loader import validate_query
                if not validate_query(spark, query_info['query']):
                    print("❌ Query validation failed. Skipping...")
                    continue
            
            # Execute query
            print(f"\nExecuting query...")
            result = spark.sql(query_info['query'])
            format_query_result(result, QUERY_CONFIG.get("max_display_rows", 20))
            
            print("✅ Query executed successfully!")
            
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            logging.error(f"Query execution failed: {e}")
        
        print("-" * 60)


def main():
    """Main function to run the Spark SQL performance demo."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="PySpark SQL Performance Demo")
    parser.add_argument(
        "--mode", 
        choices=["all", "anti_patterns", "best_practices"],
        default="all",
        help="Type of queries to run"
    )
    parser.add_argument(
        "--monitor", 
        action="store_true",
        help="Enable performance monitoring"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--list", 
        action="store_true",
        help="List all available queries"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else APP_CONFIG["log_level"]
    setup_logging(log_level)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting PySpark SQL Performance Demo - Mode: {args.mode}")
    
    # List queries if requested
    if args.list:
        list_available_queries()
        return
    
    # Create Spark session
    spark = None
    try:
        spark = create_spark_session()
        
        # Validate Spark session
        if not validate_spark_session(spark):
            logger.error("Spark session validation failed")
            return
        
        # Create sample data
        logger.info("Creating sample datasets...")
        employees_df, departments_df, projects_df, large_df = create_sample_dataframes(spark)
        
        # Register temporary views
        register_temp_views(employees_df, departments_df, projects_df, large_df)
        
        # Run queries
        run_queries(spark, args.mode)
        
        print_separator("DEMO COMPLETED")
        print("✅ All queries executed successfully!")
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"❌ Application failed: {e}")
        
    finally:
        # Stop Spark session
        if spark:
            stop_spark_session(spark)
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()
