## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early for partition pruning, reordering joins, and selecting only required columns. Broadcast hints and CTEs are used for efficient join and filter application.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and improve scan efficiency.
- Early filtering and join reordering minimize data shuffled and processed in joins.

> This PR was generated automatically. Please run tests and review carefully.
