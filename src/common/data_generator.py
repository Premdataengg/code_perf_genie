"""
Data generator for creating sample datasets.
Contains functions for generating test data for performance analysis.
"""

import logging
from typing import Tuple, List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from config.app_config import DATASET_CONFIG

logger = logging.getLogger(__name__)

# Sample data schemas
EMPLOYEES_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

DEPARTMENTS_SCHEMA = StructType([
    StructField("dept_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("manager", StringType(), True)
])

PROJECTS_SCHEMA = StructType([
    StructField("project_id", IntegerType(), True),
    StructField("project_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])

# Sample data
EMPLOYEES_DATA = [
    (1, "John Doe", "Engineering", 75000.0),
    (2, "Jane Smith", "Marketing", 65000.0),
    (3, "Mike Johnson", "Engineering", 80000.0),
    (4, "Sarah Wilson", "Sales", 60000.0),
    (5, "David Brown", "Engineering", 85000.0),
    (6, "Lisa Davis", "Marketing", 70000.0),
    (7, "Tom Miller", "Sales", 55000.0),
    (8, "Amy Taylor", "Engineering", 90000.0),
    (9, "Chris Lee", "Engineering", 82000.0),
    (10, "Emma White", "Marketing", 68000.0),
    (11, "Alex Chen", "Sales", 62000.0),
    (12, "Maria Garcia", "Engineering", 88000.0),
    (13, "James Wilson", "Marketing", 72000.0),
    (14, "Sophie Kim", "Sales", 58000.0),
    (15, "Ryan Park", "Engineering", 87000.0),
    (16, "Olivia Jones", "Marketing", 69000.0),
    (17, "Daniel Lee", "Sales", 61000.0),
    (18, "Grace Wang", "Engineering", 86000.0),
    (19, "Kevin Zhang", "Marketing", 71000.0),
    (20, "Rachel Green", "Sales", 59000.0),
    (21, "Michael Scott", "Sales", 63000.0),
    (22, "Pam Beesly", "Sales", 57000.0),
    (23, "Jim Halpert", "Sales", 64000.0),
    (24, "Dwight Schrute", "Sales", 66000.0),
    (25, "Angela Martin", "Accounting", 52000.0),
    (26, "Oscar Martinez", "Accounting", 54000.0),
    (27, "Kevin Malone", "Accounting", 50000.0),
    (28, "Toby Flenderson", "HR", 48000.0),
    (29, "Kelly Kapoor", "Customer Service", 45000.0),
    (30, "Creed Bratton", "Quality Assurance", 47000.0)
]

DEPARTMENTS_DATA = [
    ("Engineering", "Tech", "Alice Johnson"),
    ("Marketing", "Business", "Bob Smith"),
    ("Sales", "Business", "Carol White"),
    ("Accounting", "Finance", "David Miller"),
    ("HR", "Support", "Eva Rodriguez"),
    ("Customer Service", "Support", "Frank Thompson"),
    ("Quality Assurance", "Tech", "Grace Lee")
]

PROJECTS_DATA = [
    (1, "Website Redesign", "Engineering", 100000.0, "2024-01-01", "2024-06-30"),
    (2, "Marketing Campaign", "Marketing", 50000.0, "2024-02-01", "2024-05-31"),
    (3, "Sales Training", "Sales", 30000.0, "2024-03-01", "2024-04-30"),
    (4, "Mobile App", "Engineering", 150000.0, "2024-01-15", "2024-08-31"),
    (5, "Brand Awareness", "Marketing", 75000.0, "2024-02-15", "2024-07-31"),
    (6, "Customer Portal", "Engineering", 120000.0, "2024-03-15", "2024-09-30"),
    (7, "Lead Generation", "Sales", 40000.0, "2024-04-01", "2024-06-30"),
    (8, "Data Analytics", "Engineering", 80000.0, "2024-05-01", "2024-10-31"),
    (9, "Social Media", "Marketing", 60000.0, "2024-06-01", "2024-11-30"),
    (10, "Client Relations", "Sales", 35000.0, "2024-07-01", "2024-12-31")
]


def create_sample_dataframes(spark: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Create sample DataFrames for demonstration.
    
    Args:
        spark: SparkSession
        
    Returns:
        Tuple of (employees_df, departments_df, projects_df, large_df)
    """
    logger.info("Creating sample DataFrames...")
    
    # Create basic DataFrames
    employees_df = spark.createDataFrame(EMPLOYEES_DATA, EMPLOYEES_SCHEMA)
    departments_df = spark.createDataFrame(DEPARTMENTS_DATA, DEPARTMENTS_SCHEMA)
    projects_df = spark.createDataFrame(PROJECTS_DATA, PROJECTS_SCHEMA)
    
    # Create large dataset for performance testing
    large_df = generate_large_dataset(spark, DATASET_CONFIG["large_dataset_size"])
    
    logger.info(f"Created DataFrames: employees({employees_df.count()} rows), "
                f"departments({departments_df.count()} rows), "
                f"projects({projects_df.count()} rows), "
                f"large_dataset({large_df.count()} rows)")
    
    return employees_df, departments_df, projects_df, large_df


def generate_large_dataset(spark: SparkSession, num_records: int = 1000) -> DataFrame:
    """
    Generate a large dataset for performance testing.
    
    Args:
        spark: SparkSession
        num_records: Number of records to generate
        
    Returns:
        DataFrame with generated data
    """
    from pyspark.sql.functions import rand, col, when, lit
    
    logger.info(f"Generating large dataset with {num_records} records...")
    
    # Create a large dataset using Spark's built-in functions
    large_df = spark.range(num_records).select(
        col("id"),
        (col("id") % 100).alias("employee_id"),
        (col("id") % 10).alias("project_id"),
        (rand() * 100000).alias("amount"),
        when(col("id") % 3 == 0, "Engineering")
        .when(col("id") % 3 == 1, "Marketing")
        .otherwise("Sales").alias("department"),
        (col("id") % 1000).alias("transaction_id"),
        lit("2024-01-01").alias("date")
    )
    
    return large_df


def register_temp_views(
    employees_df: DataFrame,
    departments_df: DataFrame,
    projects_df: DataFrame,
    large_df: DataFrame
) -> None:
    """
    Register DataFrames as temporary views for SQL queries.
    
    Args:
        employees_df: Employees DataFrame
        departments_df: Departments DataFrame
        projects_df: Projects DataFrame
        large_df: Large dataset DataFrame
    """
    logger.info("Registering temporary views...")
    
    employees_df.createOrReplaceTempView("employees")
    departments_df.createOrReplaceTempView("departments")
    projects_df.createOrReplaceTempView("projects")
    large_df.createOrReplaceTempView("large_dataset")
    
    logger.info("Registered temporary views: employees, departments, projects, large_dataset")


def get_dataframe_info(df: DataFrame, name: str) -> Dict[str, Any]:
    """
    Get information about a DataFrame.
    
    Args:
        df: DataFrame to analyze
        name: Name of the DataFrame
        
    Returns:
        Dictionary with DataFrame information
    """
    info = {
        "name": name,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
        "schema": str(df.schema),
        "partitions": df.rdd.getNumPartitions()
    }
    
    return info


def cache_dataframe(df: DataFrame, name: str) -> DataFrame:
    """
    Cache a DataFrame for better performance.
    
    Args:
        df: DataFrame to cache
        name: Name for logging purposes
        
    Returns:
        Cached DataFrame
    """
    logger.info(f"Caching DataFrame: {name}")
    cached_df = df.cache()
    cached_df.count()  # Trigger caching
    logger.info(f"Cached DataFrame: {name} ({cached_df.count()} rows)")
    return cached_df
