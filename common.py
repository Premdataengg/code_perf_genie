"""
Common libraries and utilities for the PySpark SQL project.
This module contains shared functions, constants, and helper methods.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
APP_NAME = "Simple Spark SQL Demo"
MASTER_URL = "local[*]"

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

# Additional sample data for performance testing
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

PROJECTS_SCHEMA = StructType([
    StructField("project_id", IntegerType(), True),
    StructField("project_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])

# Large dataset for performance testing
def generate_large_dataset(spark, num_records=1000):
    """Generate a large dataset for performance testing."""
    from pyspark.sql.functions import rand, col, when, lit
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
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

def create_spark_session(app_name=None, master=None):
    """
    Create and return a Spark session with optimized configuration.
    
    Args:
        app_name (str): Application name for Spark
        master (str): Master URL for Spark cluster
        
    Returns:
        SparkSession: Configured Spark session
    """
    app_name = app_name or APP_NAME
    master = master or MASTER_URL
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .getOrCreate()
    
    # Set log level to reduce verbose output
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Created Spark session: {app_name}")
    return spark

def create_sample_dataframes(spark):
    """
    Create sample DataFrames for demonstration.
    
    Args:
        spark (SparkSession): Active Spark session
        
    Returns:
        tuple: (employees_df, departments_df, projects_df, large_df)
    """
    employees_df = spark.createDataFrame(EMPLOYEES_DATA, EMPLOYEES_SCHEMA)
    departments_df = spark.createDataFrame(DEPARTMENTS_DATA, DEPARTMENTS_SCHEMA)
    projects_df = spark.createDataFrame(PROJECTS_DATA, PROJECTS_SCHEMA)
    large_df = generate_large_dataset(spark, 1000)
    
    logger.info("Created sample DataFrames: employees, departments, projects, and large dataset")
    return employees_df, departments_df, projects_df, large_df

def register_temp_views(employees_df, departments_df, projects_df, large_df):
    """
    Register DataFrames as temporary views for SQL queries.
    
    Args:
        employees_df: Employees DataFrame
        departments_df: Departments DataFrame
        projects_df: Projects DataFrame
        large_df: Large dataset DataFrame
    """
    employees_df.createOrReplaceTempView("employees")
    departments_df.createOrReplaceTempView("departments")
    projects_df.createOrReplaceTempView("projects")
    large_df.createOrReplaceTempView("large_dataset")
    logger.info("Registered temporary views: employees, departments, projects, large_dataset")

def load_csv_data(spark, file_path, schema=None):
    """
    Load data from CSV file into a DataFrame.
    
    Args:
        spark (SparkSession): Active Spark session
        file_path (str): Path to CSV file
        schema (StructType, optional): Schema for the data
        
    Returns:
        DataFrame: Loaded data
    """
    try:
        if schema:
            df = spark.read.csv(file_path, header=True, schema=schema)
        else:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        logger.info(f"Successfully loaded CSV data from: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV data from {file_path}: {e}")
        raise

def print_separator(title=None, length=60):
    """
    Print a formatted separator line.
    
    Args:
        title (str, optional): Title to display in the separator
        length (int): Length of the separator line
    """
    if title:
        padding = (length - len(title) - 2) // 2
        print(f"\n{'=' * padding} {title} {'=' * padding}")
    else:
        print("\n" + "=" * length)

def get_sample_queries():
    """
    Get a list of sample SQL queries from the sql folder.
    
    Returns:
        list: List of sample queries with descriptions
    """
    sql_folder = "sql"
    queries = []
    
    if not os.path.exists(sql_folder):
        logger.warning(f"SQL folder '{sql_folder}' not found. Using default queries.")
        return get_default_queries()
    
    # Get all .sql files in the sql folder, sorted by filename
    sql_files = sorted([f for f in os.listdir(sql_folder) if f.endswith('.sql')])
    
    for sql_file in sql_files:
        file_path = os.path.join(sql_folder, sql_file)
        try:
            with open(file_path, 'r') as f:
                content = f.read().strip()
                
            # Extract title from filename (remove .sql and replace _ with spaces)
            title = sql_file.replace('.sql', '').replace('_', ' ').title()
            
            # Extract description from first comment line
            lines = content.split('\n')
            description = "SQL query from file"
            for line in lines:
                if line.strip().startswith('--'):
                    description = line.strip()[2:].strip()
                    break
            
            queries.append({
                "title": title,
                "query": content,
                "description": description,
                "file": sql_file
            })
            
        except Exception as e:
            logger.error(f"Error reading SQL file {sql_file}: {e}")
    
    return queries

def get_default_queries():
    """
    Get default queries if SQL folder is not available.
    
    Returns:
        list: List of default queries
    """
    return [
        {
            "title": "All employees",
            "query": "SELECT * FROM employees ORDER BY id",
            "description": "Basic SELECT query showing all employees"
        },
        {
            "title": "Engineering department employees",
            "query": """
                SELECT name, salary 
                FROM employees 
                WHERE department = 'Engineering' 
                ORDER BY salary DESC
            """,
            "description": "Filtering with WHERE clause"
        },
        {
            "title": "Average salary by department",
            "query": """
                SELECT department, 
                       COUNT(*) as employee_count,
                       AVG(salary) as avg_salary,
                       MAX(salary) as max_salary,
                       MIN(salary) as min_salary
                FROM employees 
                GROUP BY department 
                ORDER BY avg_salary DESC
            """,
            "description": "Aggregation with GROUP BY"
        },
        {
            "title": "Employees with department details",
            "query": """
                SELECT e.name, e.salary, e.department, d.category, d.manager
                FROM employees e
                JOIN departments d ON e.department = d.dept_name
                ORDER BY e.salary DESC
            """,
            "description": "JOIN operation between tables"
        },
        {
            "title": "Employee rank by salary within department",
            "query": """
                SELECT name, department, salary,
                       RANK() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank,
                       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num
                FROM employees
                ORDER BY department, salary DESC
            """,
            "description": "Window functions for ranking"
        }
    ]

def validate_query(spark, query):
    """
    Validate a SQL query without executing it.
    
    Args:
        spark (SparkSession): Active Spark session
        query (str): SQL query to validate
        
    Returns:
        bool: True if query is valid, False otherwise
    """
    try:
        # Try to create a logical plan without executing
        spark.sql(query).explain(extended=False)
        return True
    except Exception as e:
        logger.warning(f"Query validation failed: {e}")
        return False

def format_query_result(result_df, max_rows=20):
    """
    Format and display query results.
    
    Args:
        result_df: Result DataFrame
        max_rows (int): Maximum number of rows to display
    """
    if result_df is None:
        print("No results to display.")
        return
    
    count = result_df.count()
    print(f"\nQuery returned {count} row(s)")
    
    if count > 0:
        result_df.show(max_rows, truncate=False)
    else:
        print("No data found.")
