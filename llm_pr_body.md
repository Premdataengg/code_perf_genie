## Daily Spark Optimization

**Summary**
Optimized inefficient anti-pattern queries by pushing down filters, pruning columns, and using CTEs for early data reduction. Broadcast join hints and partition pruning are applied for improved performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Early filtering and column pruning reduce data shuffled in joins and aggregations
- Broadcast join used only for small dimension table
- Partition pruning and filter pushdown minimize full table scans

> This PR was generated automatically. Please run tests and review carefully.
