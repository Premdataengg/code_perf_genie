## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, reordering joins, pruning columns, reducing redundant window functions, and indicating caching for expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Pushed down filters and used CTEs for partition pruning and join reduction
- Reordered joins and added/retained broadcast hints for small tables only
- Reduced redundant window and aggregation functions for efficiency
- Indicated caching for intermediate CTEs to avoid recomputation

> This PR was generated automatically. Please run tests and review carefully.
