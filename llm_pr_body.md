## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, using column pruning, adding broadcast hints for small tables, and materializing expensive CTEs to reduce recomputation.

**Files updated**
- `src/queries/anti_patterns/01_basic_select.sql`
- `src/queries/anti_patterns/04_join.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Filter and projection pushdown reduces scanned data and memory usage
- Broadcast join hints improve join performance with small dimension tables
- Materializing CTEs simulates caching and avoids repeated expensive aggregations

> This PR was generated automatically. Please run tests and review carefully.
