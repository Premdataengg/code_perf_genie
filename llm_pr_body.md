## Daily Spark Optimization

**Summary**
Removed unnecessary DataFrame.count() calls from the sample DataFrame creation and registration process in src/common/data_generator.py. These counts triggered Spark jobs that are not needed for correctness, and their removal will reduce job startup latency and cluster load, especially for large datasets. All outputs and DataFrame structures remain unchanged.

**Files updated**
- `src/common/data_generator.py`\n

**Notes**
- No behavioral changes; only removed eager actions that are not required.
- This change is safe and incremental, and will improve startup performance.

> This PR was generated automatically. Please run tests and review carefully.
