## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns, reordering joins, materializing expensive CTEs, and replacing correlated subqueries with joins to reduce data scanned, shuffles, and memory usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Filter and column pruning reduces I/O and memory footprint
- Materializing CTEs simulates caching and avoids recomputation
- Join reordering and predicate pushdown minimize shuffle and improve execution plan
- Replacing correlated subqueries with joins prevents repeated scans

> This PR was generated automatically. Please run tests and review carefully.
