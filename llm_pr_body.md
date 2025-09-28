## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, improving join strategies with broadcast hints and filter pushdown, and suggesting caching for expensive intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column selection reduce I/O and scan costs.
- Join order and broadcast hints minimize shuffle and memory usage.
- Caching intermediate CTEs prevents redundant computation and speeds up repeated aggregations.

> This PR was generated automatically. Please run tests and review carefully.
