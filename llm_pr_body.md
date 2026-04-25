## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, using CTEs for early pruning, broadcasting small tables, caching expensive aggregations, and replacing subqueries with JOINs. These changes reduce data scanned, improve join and aggregation efficiency, and lower memory usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Early filter pushdown and column pruning reduce data shuffled and scanned
- Broadcast and caching hints improve join and aggregation performance
- Replacing subqueries with JOINs and reducing window function passes yields significant speedup

> This PR was generated automatically. Please run tests and review carefully.
