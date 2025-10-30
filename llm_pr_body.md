## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by applying partition pruning, predicate pushdown, caching, join order improvements, and reducing redundant window functions for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit column selection and partition pruning reduce scanned data and improve query speed.
- Broadcast and cached hints minimize shuffling and recomputation.
- Predicate pushdown and join reordering ensure filters are applied early, reducing join size.
- Redundant window functions consolidated to minimize computation and data shuffling.

> This PR was generated automatically. Please run tests and review carefully.
