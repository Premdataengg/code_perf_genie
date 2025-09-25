## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns early, restructuring joins, reducing redundant window functions, and replacing correlated subqueries with joins/aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition and predicate pruning reduce full table scans and data shuffling.
- Broadcast joins and join order optimization reduce shuffle and improve join performance.
- Caching and precomputing aggregations avoid repeated expensive calculations.
- Column pruning and filter pushdown minimize memory and CPU usage.

> This PR was generated automatically. Please run tests and review carefully.
