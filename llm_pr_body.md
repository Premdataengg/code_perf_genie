## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by improving partition pruning, join order, broadcast usage, window function efficiency, and simulated caching with CTE reuse.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit column selection and filter pushdown reduce data scanned and shuffled
- Broadcast and join order improvements minimize shuffle and execution time
- Window function and aggregation optimizations reduce redundant computation
- CTE reuse simulates caching, preventing recomputation of expensive results

> This PR was generated automatically. Please run tests and review carefully.
