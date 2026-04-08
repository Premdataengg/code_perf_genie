## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early for partition pruning, reordering joins, broadcasting only small tables, and caching expensive intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and early column selection reduce data scanned and shuffled.
- Join reordering and filter pushdown improve join efficiency and reduce intermediate data.
- Caching expensive CTEs avoids recomputation and speeds up repeated aggregations.

> This PR was generated automatically. Please run tests and review carefully.
