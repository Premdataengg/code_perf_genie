## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by enabling partition pruning, predicate pushdown, column pruning, reducing recomputation via CTEs, and replacing correlated subqueries with joins or precomputed CTEs. Added documentation for caching where appropriate.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and memory usage.
- Predicate pushdown and join filter reordering reduce join size and improve query speed.
- Replacing correlated subqueries and repeated subqueries with CTEs avoids redundant computation.
- Documentation for caching helps avoid recomputation in PySpark pipelines.

> This PR was generated automatically. Please run tests and review carefully.
