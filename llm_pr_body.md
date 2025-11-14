## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, predicate pushdown, column pruning, efficient join ordering, and simulated caching via CTEs to reduce recomputation and data shuffling.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition and column pruning reduces full table scans and memory usage.
- Predicate pushdown and join reordering minimize data shuffled and processed.
- Materializing expensive aggregations in CTEs simulates caching and avoids repeated computation.

> This PR was generated automatically. Please run tests and review carefully.
