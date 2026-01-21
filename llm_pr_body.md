## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying column pruning, predicate pushdown, join reordering, and suggesting caching for expensive CTEs. These changes reduce I/O, shuffle, and redundant computation while preserving identical results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Column pruning and predicate pushdown reduce data scanned and improve partition pruning.
- Join order and broadcast hints minimize shuffle and speed up joins.
- Caching intermediate aggregations avoids repeated expensive computation.

> This PR was generated automatically. Please run tests and review carefully.
