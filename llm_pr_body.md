## Daily Spark Optimization

**Summary**
Optimized inefficient anti-pattern queries by enabling partition pruning, column pruning, filter pushdown, and broadcasting small tables to reduce data movement and improve join performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and filter pushdown reduce full table scans and shuffle.
- Broadcasting and early column pruning minimize data movement and memory usage.

> This PR was generated automatically. Please run tests and review carefully.
