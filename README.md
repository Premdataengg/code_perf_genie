# PySpark SQL Performance Demo

A comprehensive PySpark project demonstrating SQL performance optimization techniques through anti-patterns and best practices.

## 🎯 Project Overview

This project showcases Spark SQL performance tuning by presenting both **anti-patterns** (what NOT to do) and **best practices** (what TO do) for optimal Spark performance. It's designed as a learning tool for understanding Spark optimization techniques.

## ✨ Features

- 🚫 **5 Performance Anti-Patterns** - Demonstrates common mistakes that hurt performance
- ✅ **5 Best Practice Solutions** - Shows optimized versions of the same queries
- 📊 **Comprehensive Sample Data** - Multiple datasets for realistic testing
- 🧩 **Modular Architecture** - Clean separation of concerns
- 📁 **Organized SQL Queries** - Categorized by performance patterns
- 🔧 **Configurable Spark Settings** - Optimized for performance testing
- 📈 **Performance Monitoring** - Built-in timing and resource tracking

## 📋 Prerequisites

- **Python 3.7+**
- **Java 8+** (required for Spark)
- **PySpark 3.5+**

## 🚀 Quick Start

### Option 1: Docker (Recommended)

The easiest way to run the demo is using Docker, which provides a consistent environment with all dependencies pre-installed.

#### Docker Architecture

This project uses the **official Apache Spark Docker image** for optimal performance and reliability:

- **Base Image**: `apache/spark:3.5.0-python3`
- **Build Time**: ~1 second (vs ~88 seconds for custom build)
- **Image Size**: 2.19GB (vs 2.78GB for custom build)
- **Reliability**: Official Apache-maintained image

#### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) installed
- [Docker Compose](https://docs.docker.com/compose/install/) installed

#### Quick Start with Docker

```bash
# Clone the repository
git clone <repository-url>
cd code_perf_genie

# Run the quick start script (recommended for first-time users)
./scripts/quick-start.sh

# Or manually:
# 1. Build the Docker image
./scripts/docker-run.sh build

# 2. Run the demo
./scripts/docker-run.sh run

# 3. View Spark UI (optional)
# Open http://localhost:4040 in your browser
```

#### Docker Commands

```bash
# Run all queries (anti-patterns and best practices)
./scripts/docker-run.sh run

# Run only anti-patterns
./scripts/docker-run.sh run --mode anti_patterns

# Run only best practices
./scripts/docker-run.sh run --mode best_practices

# Run with performance monitoring
./scripts/docker-run.sh run --monitor --verbose

# Interactive mode (bash shell in container)
./scripts/docker-run.sh run-interactive

# View logs
./scripts/docker-run.sh logs

# Stop containers
./scripts/docker-run.sh stop

# Clean up Docker resources
./scripts/docker-run.sh clean

# Get help
./scripts/docker-run.sh help
```

#### Alternative: Custom Docker Build

If you need custom Spark configurations, a `Dockerfile.custom` is available:

```bash
# Use custom Dockerfile
docker build -f Dockerfile.custom -t pyspark-custom .
```

### Option 2: Local Installation

If you prefer to run locally without Docker:

#### Prerequisites
- **Python 3.7+**
- **Java 8+** (required for Spark)
- **PySpark 3.5+**

#### Installation

```bash
# Clone the repository
git clone <repository-url>
cd code_perf_genie

# Install dependencies
pip install -r requirements.txt
```

#### Run the Demo

```bash
# Run all queries (anti-patterns and best practices)
python main.py

# Run only anti-patterns
python main.py --mode anti_patterns

# Run only best practices
python main.py --mode best_practices

# Run with performance monitoring
python main.py --monitor
```

## 📁 Project Structure

```
code_perf_genie/
├── main.py                          # Main application entry point
├── requirements.txt                 # Python dependencies
├── README.md                       # This file
├── Dockerfile                      # Docker configuration (uses official Spark image)
├── Dockerfile.custom               # Custom Docker build (for special configurations)
├── docker-compose.yml              # Docker Compose configuration
├── .dockerignore                   # Docker ignore file
├── config/
│   ├── __init__.py
│   ├── spark_config.py             # Spark configuration settings
│   └── app_config.py               # Application configuration
├── src/
│   ├── __init__.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── spark_utils.py          # Spark session and utility functions
│   │   ├── data_generator.py       # Sample data generation
│   │   └── query_loader.py         # SQL query loading utilities
│   ├── queries/
│   │   ├── __init__.py
│   │   ├── anti_patterns/          # Performance anti-pattern queries
│   │   │   ├── 01_no_partitioning.sql
│   │   │   ├── 02_inefficient_joins.sql
│   │   │   ├── 03_no_caching.sql
│   │   │   ├── 04_inefficient_aggregations.sql
│   │   │   └── 05_memory_inefficient.sql
│   │   └── best_practices/         # Optimized query solutions
│   │       ├── 01_optimized_partitioning.sql
│   │       ├── 02_optimized_joins.sql
│   │       ├── 03_optimized_caching.sql
│   │       ├── 04_optimized_aggregations.sql
│   │       └── 05_optimized_memory.sql
│   └── utils/
│       ├── __init__.py
│       ├── performance_monitor.py  # Performance monitoring utilities
│       └── output_formatter.py     # Output formatting utilities
├── scripts/
│   ├── docker-run.sh               # Docker runner script
│   └── quick-start.sh              # Quick start script
├── data/
│   ├── sample_data.csv             # Sample CSV data
│   └── generated/                  # Generated test data
├── logs/                           # Application logs
├── tests/                          # Unit tests
│   ├── __init__.py
│   ├── test_spark_utils.py
│   └── test_query_loader.py
└── docs/                           # Documentation
    ├── performance_guide.md        # Performance optimization guide
    └── api_reference.md            # API documentation
```

## 🎯 Performance Anti-Patterns

### 1. **No Partitioning Strategy**
- **File**: `src/queries/anti_patterns/01_no_partitioning.sql`
- **Problem**: Full table scan without partition pruning
- **Impact**: High I/O costs, slow execution

### 2. **Inefficient Join Strategy**
- **File**: `src/queries/anti_patterns/02_inefficient_joins.sql`
- **Problem**: Multiple large table joins without optimization
- **Impact**: High shuffle costs, memory pressure

### 3. **No Caching Strategy**
- **File**: `src/queries/anti_patterns/03_no_caching.sql`
- **Problem**: Repeated expensive computations
- **Impact**: Redundant CPU cycles, wasted resources

### 4. **Inefficient Aggregation Strategy**
- **File**: `src/queries/anti_patterns/04_inefficient_aggregations.sql`
- **Problem**: Multiple window functions with different partitions
- **Impact**: High memory usage, poor scalability

### 5. **Memory Inefficient Operations**
- **File**: `src/queries/anti_patterns/05_memory_inefficient.sql`
- **Problem**: Expensive string operations and complex calculations
- **Impact**: High memory usage, garbage collection pressure

## ✅ Best Practice Solutions

Each anti-pattern has a corresponding optimized solution demonstrating:

- **Proper partitioning strategies**
- **Broadcast joins for small tables**
- **Caching intermediate results**
- **Optimized window functions**
- **Memory-efficient operations**

## 🔧 Configuration

### Spark Configuration

The project uses optimized Spark settings in `config/spark_config.py`:

```python
SPARK_CONFIG = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
    "spark.sql.adaptive.forceOptimizeSkewedJoin": "true"
}
```

### Application Configuration

Customize application behavior in `config/app_config.py`:

```python
APP_CONFIG = {
    "dataset_size": 1000,
    "enable_monitoring": True,
    "log_level": "INFO",
    "output_format": "table"
}
```

## 📊 Sample Data

The project includes comprehensive sample data:

- **30 employees** across 7 departments
- **10 projects** with budgets and timelines
- **1000 transaction records** for performance testing
- **7 departments** with categories and managers

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_spark_utils.py

# Run with coverage
python -m pytest tests/ --cov=src
```

## 📈 Performance Monitoring

Enable performance monitoring to track:

- **Query execution time**
- **Memory usage**
- **CPU utilization**
- **Shuffle operations**
- **I/O operations**

```bash
python main.py --monitor --verbose
```

## 🛠️ Development

### Adding New Queries

1. **Anti-pattern query**:
   ```bash
   # Create new anti-pattern
   touch src/queries/anti_patterns/06_new_anti_pattern.sql
   ```

2. **Best practice solution**:
   ```bash
   # Create corresponding solution
   touch src/queries/best_practices/06_optimized_solution.sql
   ```

3. **Update documentation**:
   - Add description to this README
   - Update performance guide

### Code Style

The project follows PEP 8 standards with additional Spark-specific conventions:

- Use descriptive variable names
- Add comprehensive docstrings
- Include type hints
- Follow Spark DataFrame naming conventions

## 📚 Documentation

- **[Performance Guide](docs/performance_guide.md)** - Detailed optimization techniques
- **[API Reference](docs/api_reference.md)** - Complete API documentation
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues and solutions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Spark community for excellent documentation
- Performance optimization best practices from industry experts
- Open source contributors who provided inspiration

---

**Note**: This project is designed for educational purposes. The anti-pattern queries are intentionally inefficient to demonstrate performance issues. Always use the best practice solutions in production environments.
