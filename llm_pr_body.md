## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters for partition pruning, pruning columns early, reordering joins, and suggesting caching for expensive CTEs. Reduced unnecessary data scans and recomputation for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Pushed down filters and pruned columns for partition and column pruning
- Reordered joins and added CTEs to filter large tables early
- Suggested caching for expensive intermediate aggregations
- Used window frames to reduce recomputation

> This PR was generated automatically. Please run tests and review carefully.
