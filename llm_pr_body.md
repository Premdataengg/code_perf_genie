## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by improving partition pruning, join strategies, caching, and aggregation efficiency while preserving output. Edits focus on predicate pushdown, column pruning, broadcast hints, and reducing redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and predicate pushdown reduce data scanned and shuffled
- Broadcast joins and caching minimize expensive recomputation and network transfer
- Window function reduction and pre-aggregation lower computational overhead

> This PR was generated automatically. Please run tests and review carefully.
