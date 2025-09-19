## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, predicate pushdown, CTEs, and column pruning to reduce data scanned and shuffled, and to improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition and predicate pushdown reduces full table scans and unnecessary data movement.
- CTEs and early filtering minimize repeated computation and improve join performance.
- Broadcast hints are kept only for small tables, and only necessary columns are selected.

> This PR was generated automatically. Please run tests and review carefully.
