## Daily Spark Optimization

**Summary**
Optimized inefficient anti-pattern queries by pushing down filters for partition pruning, reordering joins, broadcasting only small tables, and pruning columns to reduce I/O.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and early filtering reduce full table scans and data shuffling.
- Join reordering and predicate pushdown minimize intermediate data size and improve join performance.

> This PR was generated automatically. Please run tests and review carefully.
