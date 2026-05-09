## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, pushing down predicates, pruning columns, adding broadcast hints, and replacing correlated subqueries with CTEs. These changes improve join efficiency, reduce I/O, and minimize memory usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and column selection reduce unnecessary data scans
- Broadcast hints and join reordering improve join performance
- CTEs replace correlated subqueries to avoid repeated computation

> This PR was generated automatically. Please run tests and review carefully.
