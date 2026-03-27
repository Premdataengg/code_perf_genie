## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters before joins, pruning columns early, and factoring out repeated subqueries into CTEs. This enables partition pruning, reduces data scanned and shuffled, and improves join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and predicate pushdown reduce full table scans
- Factoring out repeated subqueries and pruning columns early improves join and aggregation performance

> This PR was generated automatically. Please run tests and review carefully.
