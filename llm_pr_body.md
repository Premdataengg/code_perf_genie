## Daily Spark Optimization

**Summary**
Major anti-patterns were addressed by pushing down filters, pruning columns, materializing reused CTEs, optimizing join order, and reducing redundant window functions and subqueries.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition and column pruning reduces I/O and memory usage
- Materializing CTEs simulates caching, avoiding recomputation
- Filter pushdown and join order minimize shuffle and improve query speed
- Redundant window functions and subqueries eliminated for efficiency

> This PR was generated automatically. Please run tests and review carefully.
