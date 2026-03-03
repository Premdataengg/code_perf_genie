## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join strategies, and caching of expensive aggregations. These changes reduce full table scans, improve join efficiency, and minimize recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and filter pushdown for large tables
- Broadcast join hints for small dimension tables
- Caching recommendation for expensive aggregations
- Performance gain by reducing data shuffling and recomputation

> This PR was generated automatically. Please run tests and review carefully.
