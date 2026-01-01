## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by applying predicate pushdown, column pruning, join reordering, window function simplification, and simulated caching. These changes reduce data scanned, improve join and aggregation efficiency, and lower memory usage while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Pushed filters into subqueries for early data reduction
- Pruned columns to avoid SELECT * and enable columnar optimization
- Reordered joins and added broadcast hints for small tables
- Replaced expensive subquery aggregations with window functions
- Materialized expensive CTEs to simulate caching and avoid recomputation

> This PR was generated automatically. Please run tests and review carefully.
