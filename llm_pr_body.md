## Daily Spark Optimization

**Summary**
Optimized inefficient queries by enabling partition pruning, predicate pushdown, column pruning, and early filtering. Added broadcast hints and filtered small tables before joins.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and predicate pushdown reduce data scanned and shuffled.
- Broadcast join and early filtering minimize shuffle and improve join performance.

> This PR was generated automatically. Please run tests and review carefully.
