## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters for partition pruning, reordering joins and broadcasting only smallest tables, and adding caching hints for reused CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Join reordering and early filtering minimize shuffle and join cost
- Caching intermediate results avoids recomputation and speeds up repeated aggregations

> This PR was generated automatically. Please run tests and review carefully.
