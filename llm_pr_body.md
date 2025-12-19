## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by improving partition pruning, join strategies, caching, and aggregation efficiency. Changes include explicit column selection, filter pushdown, join reordering, window function reduction, and CTE reuse.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and improve partition pruning.
- Join order and broadcast hints optimize join performance and reduce shuffle.
- Caching (noted in comments) avoids recomputation of expensive CTEs.
- Window function and aggregation deduplication reduces computation and shuffling.

> This PR was generated automatically. Please run tests and review carefully.
