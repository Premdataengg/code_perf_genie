## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by introducing partition pruning, predicate pushdown, efficient join order, broadcast/caching hints, and reduced redundant window functions and aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce I/O and improve scan efficiency.
- Join order and predicate pushdown minimize shuffle and intermediate data size.
- Caching and broadcast hints reduce recomputation and optimize join strategies.
- Aggregation refactoring reduces multiple passes and expensive window operations.

> This PR was generated automatically. Please run tests and review carefully.
