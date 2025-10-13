## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing filters early, pruning columns, precomputing aggregations in CTEs, and applying caching and broadcast hints only where appropriate.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Precomputing aggregations and using CTEs minimize redundant computation
- Caching and join reordering improve query execution speed
- Avoiding subqueries in SELECT and pushing filters before joins narrows data early

> This PR was generated automatically. Please run tests and review carefully.
