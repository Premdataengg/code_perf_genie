## Daily Spark Optimization

**Summary**
The main optimization is to avoid unnecessary DataFrame.count() calls during the creation of sample DataFrames in src/common/data_generator.py. Previously, .count() was called on each DataFrame just to log the number of rows, which triggers a full scan and can slow down startup, especially with large datasets. The new version logs only column counts and schema info at creation, and only triggers .count() when explicitly requested (e.g., in get_dataframe_info). The cache_dataframe function no longer triggers .count() upon caching, which is safer and more efficient for large or rarely reused DataFrames.

**Files updated**
- `src/common/data_generator.py`\n

**Notes**
- Avoids triggering Spark actions (like count) unless needed for correctness.
- Behavior and outputs remain identical; only logging and eager actions are affected.

> This PR was generated automatically. Please run tests and review carefully.
