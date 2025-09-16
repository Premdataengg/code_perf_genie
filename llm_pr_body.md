## Daily Spark Optimization

**Summary**
Optimized inefficient partitioning and join strategies by enabling partition pruning, predicate pushdown, and join reordering with CTEs and early filtering.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage.
- Early filtering and join reordering minimize shuffle and intermediate data size, improving query speed.

> This PR was generated automatically. Please run tests and review carefully.
