## Daily Spark Optimization

**Summary**
The anti-pattern SQL queries have been incrementally optimized for Spark SQL performance. Major improvements include broadcast join hints for small dimension tables, early filter pushdown, partition pruning, column pruning, and the use of CTEs to materialize expensive aggregations. Expensive operations such as unnecessary subqueries, redundant window functions, and full table scans have been removed or replaced with efficient alternatives. These changes reduce shuffle, improve memory usage, and ensure queries are partition-aware, all while preserving output correctness.

**Files updated**
- `src/queries/anti_patterns/01_basic_select.sql`
- `src/queries/anti_patterns/02_filtering.sql`
- `src/queries/anti_patterns/03_aggregation.sql`
- `src/queries/anti_patterns/04_join.sql`
- `src/queries/anti_patterns/05_window_functions.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- JOIN optimization
- Partitioning improvement
- Shuffle reduction

> This PR was generated automatically. Please run tests and review carefully.
