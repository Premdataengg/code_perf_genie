## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by introducing partition pruning, broadcast join hints, and caching of expensive aggregations to improve Spark SQL performance while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning reduces full table scans and improves query speed.
- Broadcast join hints optimize join order and reduce shuffle for small tables.
- Caching expensive CTEs avoids recomputation and speeds up downstream queries.

> This PR was generated automatically. Please run tests and review carefully.
