## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, using CTEs for partition pruning and simulated caching, and applying broadcast hints only to small tables.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column selection reduce data scan and shuffle.
- Early filtering and CTEs avoid recomputation and improve query efficiency.
- Broadcast join hints are used only for small dimension tables.

> This PR was generated automatically. Please run tests and review carefully.
