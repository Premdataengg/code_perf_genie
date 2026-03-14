## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by introducing partition pruning, predicate pushdown, broadcast hints, and simulated caching of expensive aggregations. These changes reduce data scanned, shuffle, and redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and limiting output rows reduce full table scans and memory usage.
- Predicate pushdown and broadcast join hints minimize shuffle and join costs.
- Materializing expensive CTEs simulates caching, avoiding repeated computation.

> This PR was generated automatically. Please run tests and review carefully.
