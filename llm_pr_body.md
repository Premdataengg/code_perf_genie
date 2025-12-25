## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by applying early filtering, pruning columns, reusing expensive computations with CTEs, improving join order, and reducing redundant window/aggregation operations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter and column pruning reduces data scanned and shuffled
- CTE reuse simulates caching and avoids recomputation
- Broadcast hints and join order improve join efficiency
- Window and aggregation optimizations reduce computation cost

> This PR was generated automatically. Please run tests and review carefully.
