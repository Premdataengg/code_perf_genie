## Daily Spark Optimization

**Summary**
Optimized inefficient join, partitioning, and repeated scan anti-patterns by pushing filters early, materializing filtered CTEs, and using broadcast hints only for small tables.

**Files updated**
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Join order and predicate pushdown reduce shuffle and data scanned
- Partition pruning and CTE materialization minimize unnecessary computation
- Broadcast hints are used only for small tables, improving join efficiency

> This PR was generated automatically. Please run tests and review carefully.
