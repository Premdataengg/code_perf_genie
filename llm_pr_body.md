## Daily Spark Optimization

**Summary**
Optimized inefficient queries by pushing filters early for partition pruning, selecting only necessary columns, and reordering joins for better performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Early filter pushdown and column pruning reduce scan size and improve partition pruning.
- Join reordering and broadcast hints optimize join execution and reduce shuffle.

> This PR was generated automatically. Please run tests and review carefully.
