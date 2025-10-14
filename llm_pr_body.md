## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, column pruning, filter pushdown, join order improvements, and simulated caching to reduce redundant computation and data shuffling.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Filter pushdown and join order minimize data shuffling and improve join efficiency
- Simulated caching avoids recomputation of expensive aggregations

> This PR was generated automatically. Please run tests and review carefully.
