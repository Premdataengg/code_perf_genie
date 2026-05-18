## Daily Spark Optimization

**Summary**
Optimized inefficient anti-pattern queries by pushing filters before JOINs for predicate and partition pruning, using CTEs, column pruning, and adding broadcast hints where appropriate.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushed filters into CTEs for early reduction of data scanned and improved partition pruning.
- Added BROADCAST hint to projects for optimal join strategy if small.
- Column pruning reduces unnecessary data movement.
- Predicate pushdown and join order optimization yield significant performance gains.

> This PR was generated automatically. Please run tests and review carefully.
