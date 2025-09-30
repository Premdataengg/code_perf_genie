## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enforcing partition pruning, predicate pushdown, column pruning, join order, and simulated caching of expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage.
- Early filtering and join order minimize data shuffling and join cost.
- Simulated caching avoids repeated computation of expensive aggregations.

> This PR was generated automatically. Please run tests and review carefully.
