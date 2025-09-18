## Daily Spark Optimization

**Summary**
Optimized inefficient anti-pattern queries by pushing filters early for partition pruning, reordering joins, and selecting only required columns. Broadcast hints are used only for small tables.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and early filtering reduce scan size and improve query speed
- Join order and predicate pushdown minimize shuffle and memory usage

> This PR was generated automatically. Please run tests and review carefully.
