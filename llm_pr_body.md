## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying partition pruning, predicate pushdown, column pruning, join and aggregation best practices, and replacing scalar subqueries with joins or CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Key improvement: Early filtering, column pruning, and materializing expensive CTEs to simulate caching.
- Performance gain: Reduced data scanned, minimized shuffles, avoided repeated computation, and improved join and aggregation efficiency.

> This PR was generated automatically. Please run tests and review carefully.
