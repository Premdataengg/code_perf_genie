## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early for partition pruning, using CTEs for predicate pushdown, and selecting only necessary columns to reduce I/O and shuffle.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and predicate pushdown reduce full table scans and improve query speed.
- Early filtering and efficient join order minimize shuffle and memory usage.

> This PR was generated automatically. Please run tests and review carefully.
