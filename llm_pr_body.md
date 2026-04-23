## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by enabling partition pruning, improving join strategies with predicate pushdown and broadcast hints, and suggesting caching for expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and predicate pushdown reduce data scanned and improve query speed.
- Broadcast hints and join reordering optimize join execution plans.
- Caching intermediate CTEs avoids redundant computation and saves memory/CPU.

> This PR was generated automatically. Please run tests and review carefully.
