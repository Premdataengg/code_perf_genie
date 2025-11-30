## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing filters early, materializing reused subqueries, pruning columns, and improving join and aggregation strategies for Spark SQL.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column selection reduce I/O and improve scan efficiency.
- Materializing expensive CTEs simulates caching for reused computations.
- Precomputing aggregations and minimizing window functions reduces redundant calculations.
- Early filtering and join reordering reduce shuffle and memory usage.

> This PR was generated automatically. Please run tests and review carefully.
