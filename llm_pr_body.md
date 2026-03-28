## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early, reusing filtered CTEs, adding caching hints, and reordering joins for better partition pruning and reduced data shuffling.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Early filter pushdown and column pruning for partition pruning
- CTE reuse avoids repeated scans and reduces memory usage
- Caching expensive aggregations prevents recomputation
- Join reordering and predicate pushdown reduce shuffle and improve performance

> This PR was generated automatically. Please run tests and review carefully.
