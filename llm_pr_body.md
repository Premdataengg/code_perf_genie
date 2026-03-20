## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, column pruning, filter pushdown, and simulated caching of expensive CTEs to reduce recomputation and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage.
- Filter pushdown and CTE materialization (simulated caching) avoid redundant computation and improve query speed.

> This PR was generated automatically. Please run tests and review carefully.
