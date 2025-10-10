## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing filters and column pruning into CTEs, materializing expensive aggregations, optimizing join order, and reducing redundant window and subquery computations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filtering and column pruning reduce data shuffling and memory usage.
- Materializing expensive aggregations simulates caching and avoids recomputation.
- Broadcasting small tables and pushing predicates before joins improve join efficiency.
- Replacing subqueries with window functions and limiting window calculations reduces computation overhead.

> This PR was generated automatically. Please run tests and review carefully.
