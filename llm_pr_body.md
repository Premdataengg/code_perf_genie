## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, predicate pushdown, join reordering, column pruning, and adding caching hints for expensive CTEs. These changes reduce full scans, avoid recomputation, and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Column pruning and predicate pushdown reduce data scanned and improve partition pruning.
- Join reordering and broadcast hinting minimize shuffle and optimize join execution.
- Caching intermediate results avoids recomputation and speeds up repeated aggregations.

> This PR was generated automatically. Please run tests and review carefully.
