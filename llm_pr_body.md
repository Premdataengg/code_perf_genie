## Daily Spark Optimization

**Summary**
Optimized two major anti-pattern queries by eliminating redundant scans and enabling partition pruning, significantly reducing unnecessary computation and improving performance.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`


**Notes**
- Pushed repeated employee filters into a CTE for reuse in multi-way self-joins.
- Moved partition-pruning filters directly into the main query for large_dataset to enable Spark's partition pruning and avoid full table scans.

> This PR was generated automatically. Please run tests and review carefully.
