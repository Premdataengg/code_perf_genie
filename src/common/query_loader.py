"""
Query loader for managing SQL queries.
Contains functions for loading and organizing SQL queries from files.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


def get_sample_queries(query_type: str = "all") -> List[Dict[str, Any]]:
    """
    Get sample SQL queries from the queries directory.
    
    Args:
        query_type: Type of queries to load ("all", "anti_patterns", "best_practices")
        
    Returns:
        List of query dictionaries with metadata
    """
    queries = []
    
    # Define query directories
    base_dir = Path("src/queries")
    anti_patterns_dir = base_dir / "anti_patterns"
    best_practices_dir = base_dir / "best_practices"
    
    if query_type in ["all", "anti_patterns"]:
        queries.extend(_load_queries_from_directory(anti_patterns_dir, "anti_patterns"))
    
    if query_type in ["all", "best_practices"]:
        queries.extend(_load_queries_from_directory(best_practices_dir, "best_practices"))
    
    # Sort queries by filename
    queries.sort(key=lambda x: x.get("file", ""))
    
    logger.info(f"Loaded {len(queries)} queries of type: {query_type}")
    return queries


def _load_queries_from_directory(directory: Path, query_type: str) -> List[Dict[str, Any]]:
    """
    Load queries from a specific directory.
    
    Args:
        directory: Directory path to load queries from
        query_type: Type of queries being loaded
        
    Returns:
        List of query dictionaries
    """
    queries = []
    
    if not directory.exists():
        logger.warning(f"Query directory not found: {directory}")
        return queries
    
    # Get all .sql files in the directory, sorted by filename
    sql_files = sorted([f for f in directory.iterdir() if f.suffix == '.sql'])
    
    for sql_file in sql_files:
        try:
            query_info = _load_single_query(sql_file, query_type)
            if query_info:
                queries.append(query_info)
                
        except Exception as e:
            logger.error(f"Error loading query file {sql_file}: {e}")
    
    return queries


def _load_single_query(file_path: Path, query_type: str) -> Optional[Dict[str, Any]]:
    """
    Load a single SQL query from file.
    
    Args:
        file_path: Path to SQL file
        query_type: Type of query
        
    Returns:
        Query dictionary with metadata
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        if not content:
            logger.warning(f"Empty query file: {file_path}")
            return None
        
        # Extract title from filename (remove .sql and replace _ with spaces)
        title = file_path.stem.replace('_', ' ').title()
        
        # Extract description from first comment line
        lines = content.split('\n')
        description = f"{query_type.replace('_', ' ').title()} query"
        
        for line in lines:
            if line.strip().startswith('--'):
                description = line.strip()[2:].strip()
                break
        
        query_info = {
            "title": title,
            "query": content,
            "description": description,
            "file": file_path.name,
            "type": query_type,
            "path": str(file_path)
        }
        
        return query_info
        
    except Exception as e:
        logger.error(f"Error reading SQL file {file_path}: {e}")
        return None


def validate_query(spark, query: str) -> bool:
    """
    Validate a SQL query without executing it.
    
    Args:
        spark: SparkSession
        query: SQL query to validate
        
    Returns:
        True if query is valid, False otherwise
    """
    try:
        # Try to create a logical plan without executing
        spark.sql(query).explain(extended=False)
        return True
    except Exception as e:
        logger.warning(f"Query validation failed: {e}")
        return False


def format_query_result(result_df, max_rows: int = 20) -> None:
    """
    Format and display query results.
    
    Args:
        result_df: Result DataFrame
        max_rows: Maximum number of rows to display
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


def print_separator(title: Optional[str] = None, length: int = 60) -> None:
    """
    Print a formatted separator line.
    
    Args:
        title: Title to display in the separator
        length: Length of the separator line
    """
    if title:
        padding = (length - len(title) - 2) // 2
        print(f"\n{'=' * padding} {title} {'=' * padding}")
    else:
        print("\n" + "=" * length)


def get_query_statistics(queries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Get statistics about loaded queries.
    
    Args:
        queries: List of query dictionaries
        
    Returns:
        Dictionary with query statistics
    """
    stats = {
        "total_queries": len(queries),
        "anti_patterns": len([q for q in queries if q.get("type") == "anti_patterns"]),
        "best_practices": len([q for q in queries if q.get("type") == "best_practices"]),
        "total_characters": sum(len(q.get("query", "")) for q in queries),
        "average_query_length": sum(len(q.get("query", "")) for q in queries) / len(queries) if queries else 0
    }
    
    return stats


def list_available_queries() -> None:
    """
    List all available queries with their metadata.
    """
    queries = get_sample_queries("all")
    
    print("\n" + "=" * 80)
    print("AVAILABLE QUERIES")
    print("=" * 80)
    
    for i, query in enumerate(queries, 1):
        print(f"\n{i}. {query['title']}")
        print(f"   Type: {query['type'].replace('_', ' ').title()}")
        print(f"   File: {query['file']}")
        print(f"   Description: {query['description']}")
        print(f"   Length: {len(query['query'])} characters")
    
    stats = get_query_statistics(queries)
    print(f"\n" + "=" * 80)
    print(f"TOTAL: {stats['total_queries']} queries "
          f"({stats['anti_patterns']} anti-patterns, "
          f"{stats['best_practices']} best practices)")
    print("=" * 80)
