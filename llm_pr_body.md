## Daily Spark Optimization

**Summary**
Optimized inefficient queries by pushing filters for partition pruning, reordering joins, and broadcasting only small tables. Also pruned columns to minimize data shuffling.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and early filtering reduce data scanned and shuffled
- Join order and predicate pushdown improve join efficiency and overall query speed

> This PR was generated automatically. Please run tests and review carefully.
