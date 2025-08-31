## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns, using CTEs for reuse, and applying broadcast hints only to small tables. This reduces shuffle, memory usage, and improves partition pruning.

**Files updated**
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Join order and predicate pushdown reduce data shuffled and scanned
- Column pruning and CTEs minimize memory and computation
- Broadcast hints are used only for small tables

> This PR was generated automatically. Please run tests and review carefully.
