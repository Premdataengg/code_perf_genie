## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing filters early, pruning columns, reordering joins, caching expensive CTEs, and reducing redundant window/aggregation operations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and column selection reduce I/O and shuffle.
- Join order and predicate pushdown minimize intermediate data size.
- Caching and CTE pre-aggregation avoid recomputation and lower memory usage.
- Window and aggregation optimizations reduce computation passes.

> This PR was generated automatically. Please run tests and review carefully.
