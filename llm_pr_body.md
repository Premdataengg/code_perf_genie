## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early for partition pruning, using CTEs to avoid repeated scans, improving join order, and pruning columns. These changes reduce data shuffling and improve query execution efficiency.

**Files updated**
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filtering and CTEs reduce full scans and redundant computation
- Join order and column pruning minimize shuffle and memory usage

> This PR was generated automatically. Please run tests and review carefully.
