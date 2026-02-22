## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, pruning columns, caching expensive CTEs, and pre-aggregating window functions to reduce data shuffling and recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage.
- Caching and pre-aggregation minimize redundant computation and improve query speed.

> This PR was generated automatically. Please run tests and review carefully.
