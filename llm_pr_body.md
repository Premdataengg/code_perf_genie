## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying column pruning, partition pruning, join reordering, broadcast hints, predicate pushdown, and added caching comments for expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Column pruning and early filtering reduce data scanned and shuffled
- Join order and broadcast hints minimize shuffle and improve join performance
- Caching intermediate results avoids recomputation and speeds up repeated aggregations

> This PR was generated automatically. Please run tests and review carefully.
