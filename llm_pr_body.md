## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, predicate pushdown, column pruning, and efficient join order with broadcast hints. This reduces data scanned, shuffles, and memory usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Added explicit column selection and partition pruning for large tables
- Moved filters into subqueries for early reduction of data volume
- Applied broadcast join only to small dimension table
- Performance gain: less data shuffled, improved join efficiency, reduced memory footprint

> This PR was generated automatically. Please run tests and review carefully.
