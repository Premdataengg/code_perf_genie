## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying partition pruning, predicate pushdown, join order improvements, broadcast hints, column pruning, caching, and efficient aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and predicate pushdown reduce data scanned and improve query speed.
- Join order and broadcast hints minimize shuffle and memory usage.
- Caching and pre-aggregation avoid recomputation and lower resource consumption.
- Column pruning ensures only necessary data is processed.

> This PR was generated automatically. Please run tests and review carefully.
