## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, pruning columns, recommending caching, and improving window function usage for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and shuffled.
- Broadcast join and CTE caching recommendations improve join and aggregation efficiency.
- Efficient window frame for running_total minimizes computation overhead.

> This PR was generated automatically. Please run tests and review carefully.
