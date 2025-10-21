## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters for partition pruning, reordering joins and filtering early, and caching expensive intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition and column pruning reduces data scanned and improves query speed.
- Early filtering and join reordering minimize data shuffled and processed.
- Caching expensive CTEs avoids redundant computation and speeds up downstream queries.

> This PR was generated automatically. Please run tests and review carefully.
