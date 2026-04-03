## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying predicate and column pruning, factoring out repeated subqueries, and improving window function efficiency. Filters are now pushed down and only necessary columns are selected.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition and predicate pruning reduce full table scans
- Column pruning and CTEs minimize data shuffling and computation
- Factoring out repeated subqueries avoids redundant computation

> This PR was generated automatically. Please run tests and review carefully.
